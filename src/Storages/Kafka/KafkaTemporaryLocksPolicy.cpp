#include <Storages/Kafka/KafkaTemporaryLocksPolicy.h>

#include <algorithm>

namespace DB
{

size_t computeNodeQuota(const size_t topic_partitions_count, const size_t active_replica_count)
{
    return std::max<size_t>(topic_partitions_count / std::max<size_t>(active_replica_count, 1), 1);
}

size_t computeConsumerQuota(const size_t node_quota, const size_t num_consumers, const size_t idx)
{
    if (num_consumers <= 1)
        return node_quota;
    return node_quota / num_consumers + (idx < node_quota % num_consumers ? 1 : 0);
}

ReplicaLocksSummary summarizeReplicaLocks(
    const std::vector<String> & active_replicas,
    const std::unordered_map<String, size_t> & lock_count_by_replica,
    const String & own_replica,
    const size_t topic_partitions_count)
{
    ReplicaLocksSummary summary;
    /// Our own `is_active` may be transiently missing, and 0 would divide by zero.
    summary.active_replica_count = std::max<size_t>(active_replicas.size(), 1);

    const size_t node_quota = computeNodeQuota(topic_partitions_count, summary.active_replica_count);
    for (const auto & replica : active_replicas)
    {
        const auto it = lock_count_by_replica.find(replica);
        const size_t lock_count = it == lock_count_by_replica.end() ? 0 : it->second;
        if (lock_count == 0)
            summary.has_replica_without_locks = true;
        /// Counting our own replica here would make it wait for itself forever if one of its consumers could not be
        /// created. Our own consumers report to `ConsumerLocksRegistry` instead.
        else if (replica != own_replica && lock_count < node_quota)
            summary.has_other_replica_below_quota = true;
    }
    return summary;
}

TemporaryLockSlots::Slot::Slot(std::shared_ptr<TemporaryLockSlots> slots_)
    : slots(std::move(slots_))
{
}

TemporaryLockSlots::Slot::Slot(Slot && other) noexcept
    : slots(std::move(other.slots))
{
}

TemporaryLockSlots::Slot & TemporaryLockSlots::Slot::operator=(Slot && other) noexcept
{
    if (this != &other)
    {
        release();
        slots = std::move(other.slots);
    }
    return *this;
}

TemporaryLockSlots::Slot::~Slot()
{
    release();
}

void TemporaryLockSlots::Slot::release() noexcept
{
    if (slots)
    {
        slots->used_slots.fetch_sub(1);
        slots.reset();
    }
}

std::optional<TemporaryLockSlots::Slot> TemporaryLockSlots::tryAcquire(const size_t limit)
{
    size_t used = used_slots.load();
    while (used < limit)
    {
        if (used_slots.compare_exchange_weak(used, used + 1))
            return Slot(shared_from_this());
    }
    return std::nullopt;
}

ConsumerLocksRegistry::ConsumerLocksRegistry(const size_t num_consumers_)
    : num_consumers(std::max<size_t>(num_consumers_, 1))
    , temporary_lock_slots(std::make_shared<TemporaryLockSlots>())
    , consumers(num_consumers)
{
}

void ConsumerLocksRegistry::addConsumer(const size_t idx)
{
    std::lock_guard lock(mutex);
    if (idx < consumers.size())
        consumers[idx].emplace();
}

void ConsumerLocksRegistry::removeConsumer(const size_t idx)
{
    std::lock_guard lock(mutex);
    if (idx < consumers.size())
        consumers[idx].reset();
}

void ConsumerLocksRegistry::reportPermanentLocks(const size_t idx, const size_t permanent_locks, const size_t permanent_quota)
{
    std::lock_guard lock(mutex);
    if (idx < consumers.size() && consumers[idx].has_value())
    {
        consumers[idx]->permanent_locks = permanent_locks;
        consumers[idx]->permanent_quota = permanent_quota;
    }
}

void ConsumerLocksRegistry::forgetPermanentLocks(const size_t idx)
{
    std::lock_guard lock(mutex);
    if (idx < consumers.size() && consumers[idx].has_value())
        consumers[idx]->permanent_locks.reset();
}

bool ConsumerLocksRegistry::hasOtherConsumerBelowQuota(const size_t idx) const
{
    std::lock_guard lock(mutex);
    for (size_t i = 0; i < consumers.size(); ++i)
    {
        if (i == idx || !consumers[i].has_value() || !consumers[i]->permanent_locks.has_value())
            continue;
        if (*consumers[i]->permanent_locks < consumers[i]->permanent_quota)
            return true;
    }
    return false;
}

size_t ConsumerLocksRegistry::getUnusedPermanentQuotaLocked(const size_t node_quota) const
{
    size_t unused_permanent_quota = 0;
    for (size_t i = 0; i < consumers.size(); ++i)
        if (!consumers[i].has_value())
            unused_permanent_quota += computeConsumerQuota(node_quota, num_consumers, i);
    return unused_permanent_quota;
}

bool ConsumerLocksRegistry::isLeastLoadedConsumer(const size_t idx, const size_t node_quota) const
{
    std::lock_guard lock(mutex);
    if (getUnusedPermanentQuotaLocked(node_quota) > 0)
        return true;

    std::optional<size_t> min_quota;
    for (size_t i = 0; i < consumers.size(); ++i)
    {
        if (!consumers[i].has_value())
            continue;
        const size_t quota = computeConsumerQuota(node_quota, num_consumers, i);
        min_quota = std::min(min_quota.value_or(quota), quota);
    }
    return !min_quota.has_value() || computeConsumerQuota(node_quota, num_consumers, idx) <= *min_quota;
}

size_t ConsumerLocksRegistry::getTemporaryLockSlotsLimit(const size_t node_quota) const
{
    std::lock_guard lock(mutex);
    return 1 + getUnusedPermanentQuotaLocked(node_quota);
}

std::optional<TemporaryLockSlots::Slot> ConsumerLocksRegistry::tryAcquireTemporaryLockSlot(const size_t node_quota)
{
    return temporary_lock_slots->tryAcquire(getTemporaryLockSlotsLimit(node_quota));
}

TemporaryLocksQuota computeTemporaryLocksQuota(const TemporaryLocksQuotaInput & input)
{
    using enum TemporaryLocksQuotaReason;

    if (input.available_topic_partitions == 0)
        return {.quota = 0, .reason = NoAvailableTopicPartitions, .needs_slot = false};

    /// The consumer below its quota only takes free topic partitions, so a temporary lock taken now would block it for
    /// at least another round.
    if (input.has_consumer_below_quota && input.rounds_with_consumer_below_quota <= MAX_ROUNDS_TO_YIELD_TO_CONSUMER_BELOW_QUOTA)
        return {.quota = 0, .reason = YieldToConsumerBelowQuota, .needs_slot = false};

    if (input.previous_quota > 0 && (input.has_replica_without_locks || input.has_consumer_below_quota))
        return {.quota = 0, .reason = GiveOtherConsumersAChance, .needs_slot = false};

    /// Covering a replica that holds no locks must not depend on the slots of the replica: the replica holds more than
    /// `node_quota + 1` locks only while another active replica holds none.
    if (input.has_replica_without_locks)
        return {.quota = 1, .reason = CoverReplicaWithoutLocks, .needs_slot = false};

    if (!input.is_least_loaded_consumer)
        return {.quota = 0, .reason = NotLeastLoadedConsumer, .needs_slot = false};

    return {.quota = 1, .reason = Granted, .needs_slot = true};
}

}
