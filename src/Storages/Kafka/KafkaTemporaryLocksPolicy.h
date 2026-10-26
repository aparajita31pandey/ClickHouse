#pragma once

#include <base/types.h>
#include <base/defines.h>

#include <atomic>
#include <memory>
#include <mutex>
#include <optional>
#include <unordered_map>
#include <vector>

namespace DB
{

/// Decides how many temporary topic partition locks a consumer of `StorageKafka2` may take in a round.
///
/// Permanent locks give every active replica `node_quota = max(P / R, 1)` topic partitions, split between its consumers.
/// Temporary locks cover what the permanent locks leave free, which in a healthy cluster is the `P mod R` leftover. A
/// consumer drops its temporary locks at the beginning of every round and decides here whether to take one again.
///
/// Before this policy existed, two things went wrong after a replica was lost:
///   - The holder of a temporary lock released it and took it back in the same round. A consumer below its permanent
///     quota only got that topic partition if its own round happened to fall into the short window in between, so the
///     cluster needed hours to converge to the permanent quotas.
///   - The temporary quota of every consumer grew by one per round without any limit per replica, so one replica could
///     hold 37 topic partitions while the other held 27, and single consumers held 4-5 topic partitions.
///
/// The policy has two modes:
///   - Every active replica holds locks: a replica holds at most its permanent capacity plus one lock, enforced by
///     `TemporaryLockSlots`, nobody takes a temporary lock while a consumer is below its permanent quota, and the
///     temporary lock goes to a consumer with the smallest permanent quota of the replica. `poll` serves the topic
///     partitions of a consumer round-robin, so a third topic partition on a consumer that already holds two slows down
///     all three.
///   - Some active replica holds no locks: it might be starting up, or it might not consume at all (e.g. no materialized
///     view is attached), so covering its topic partitions matters more than balance. The behaviour from before the
///     policy is kept: every consumer takes one temporary lock every other round, without a limit per replica.

/// The permanent quota of a replica.
size_t computeNodeQuota(size_t topic_partitions_count, size_t active_replica_count);

/// The share of `node_quota` of the consumer `idx`. The remainder goes to the lowest indices, so the quotas of all
/// `num_consumers` consumers sum up to exactly `node_quota`.
size_t computeConsumerQuota(size_t node_quota, size_t num_consumers, size_t idx);

/// What a consumer knows about the locks of the active replicas, collected from the lock nodes in Keeper.
struct ReplicaLocksSummary
{
    /// Number of active replicas, never less than 1.
    size_t active_replica_count = 1;
    /// An active replica holds no locks at all.
    bool has_replica_without_locks = false;
    /// Another active replica holds some locks, but fewer than its permanent quota. It consumes, and it is waiting for
    /// topic partitions that are free or held as temporary locks.
    bool has_other_replica_below_quota = false;
};

/// `active_replicas` are the active replicas of the shard, `lock_count_by_replica` maps the replica names stored in
/// the lock nodes to the number of locks they hold. Replicas that hold locks but are not active are ignored. The own
/// replica is never reported as below its quota, its consumers are tracked by `ConsumerLocksRegistry`.
ReplicaLocksSummary summarizeReplicaLocks(
    const std::vector<String> & active_replicas,
    const std::unordered_map<String, size_t> & lock_count_by_replica,
    const String & own_replica,
    size_t topic_partitions_count);

/// A counter of the temporary locks held by the consumers of one storage on this replica. A consumer acquires a slot
/// before it creates a temporary lock in Keeper. The check and the increment are a single atomic operation, so two
/// consumers that run their rounds at the same moment cannot both take the last slot.
///
/// Must be created with `std::make_shared`: a `Slot` keeps the counter alive, so it may outlive the registry.
class TemporaryLockSlots : public std::enable_shared_from_this<TemporaryLockSlots>
{
public:
    /// Owns one slot and gives it back when destroyed. It is stored next to the lock it was acquired for, so the slot
    /// is released on every path that drops the lock: the next round, a new Keeper session, the consumer's destruction,
    /// or an exception.
    class Slot
    {
    public:
        Slot(Slot && other) noexcept;
        Slot & operator=(Slot && other) noexcept;
        Slot(const Slot &) = delete;
        Slot & operator=(const Slot &) = delete;
        ~Slot();

    private:
        friend class TemporaryLockSlots;
        explicit Slot(std::shared_ptr<TemporaryLockSlots> slots_);
        void release() noexcept;

        std::shared_ptr<TemporaryLockSlots> slots;
    };

    /// Returns a slot if fewer than `limit` slots are in use.
    std::optional<Slot> tryAcquire(size_t limit);
    size_t used() const { return used_slots.load(); }

private:
    std::atomic<size_t> used_slots{0};
};

using TemporaryLockSlotsPtr = std::shared_ptr<TemporaryLockSlots>;

/// State shared by the consumers of one storage on this replica. Keeper only tells which replica holds a lock, not which
/// of its consumers does, and the number of temporary locks of the replica must be limited without a race.
class ConsumerLocksRegistry
{
public:
    explicit ConsumerLocksRegistry(size_t num_consumers);

    /// The consumer was created. Consumers that could not be created leave their permanent quota unused, and the
    /// replica may cover it with temporary locks, see `getTemporaryLockSlotsLimit`.
    void addConsumer(size_t idx);
    /// The consumer was destroyed.
    void removeConsumer(size_t idx);

    /// The permanent locks the consumer holds after updating them in the current round.
    void reportPermanentLocks(size_t idx, size_t permanent_locks, size_t permanent_quota);
    /// The consumer holds no locks anymore, e.g. its Keeper session was replaced.
    void forgetPermanentLocks(size_t idx);

    /// Another consumer of the storage reported fewer permanent locks than its quota.
    bool hasOtherConsumerBelowQuota(size_t idx) const;

    /// The consumer has the smallest permanent quota among the existing consumers, so a temporary lock adds the least
    /// load there. If some consumers could not be created, every consumer qualifies: the replica may hold several
    /// temporary locks then, at most one per consumer, and the consumers with the smallest quota might not be enough.
    bool isLeastLoadedConsumer(size_t idx, size_t node_quota) const;

    /// One temporary lock for the replica, plus the permanent quota of the consumers that could not be created. Then the
    /// replica holds at most `node_quota + 1` locks, whatever happened to its consumers.
    size_t getTemporaryLockSlotsLimit(size_t node_quota) const;
    std::optional<TemporaryLockSlots::Slot> tryAcquireTemporaryLockSlot(size_t node_quota);
    size_t getUsedTemporaryLockSlots() const { return temporary_lock_slots->used(); }

private:
    size_t getUnusedPermanentQuotaLocked(size_t node_quota) const TSA_REQUIRES(mutex);

    struct Consumer
    {
        /// Empty until the consumer reports its locks, and after its locks are forgotten.
        std::optional<size_t> permanent_locks;
        size_t permanent_quota = 0;
    };

    const size_t num_consumers;
    const TemporaryLockSlotsPtr temporary_lock_slots;

    mutable std::mutex mutex;
    /// Indexed by consumer index, empty for consumers that do not exist.
    std::vector<std::optional<Consumer>> consumers TSA_GUARDED_BY(mutex);
};

using ConsumerLocksRegistryPtr = std::shared_ptr<ConsumerLocksRegistry>;

struct TemporaryLocksQuotaInput
{
    /// The quota of the previous round of this consumer.
    size_t previous_quota = 0;
    /// Free topic partitions after this consumer updated its permanent locks.
    size_t available_topic_partitions = 0;
    bool has_replica_without_locks = false;
    /// Another active replica or another consumer of this storage is below its permanent quota.
    bool has_consumer_below_quota = false;
    /// Consecutive rounds of this consumer, including the current one, in which `has_consumer_below_quota` was set.
    size_t rounds_with_consumer_below_quota = 0;
    /// See `ConsumerLocksRegistry::isLeastLoadedConsumer`.
    bool is_least_loaded_consumer = true;
};

enum class TemporaryLocksQuotaReason : uint8_t
{
    /// Every active replica holds locks: one temporary lock, if the replica has a free slot.
    Granted,
    /// An active replica holds no locks: one temporary lock to cover its topic partitions, not limited by the slots.
    CoverReplicaWithoutLocks,
    NoAvailableTopicPartitions,
    /// Leave the free topic partitions to the consumer below its permanent quota.
    YieldToConsumerBelowQuota,
    /// Every other round, give a replica without locks or a consumer below its quota a chance to lock the free topic
    /// partitions.
    GiveOtherConsumersAChance,
    /// Another consumer of this storage has a smaller permanent quota and should take the temporary lock.
    NotLeastLoadedConsumer,
    /// Other consumers of this storage already hold all temporary lock slots of the replica.
    NoTemporaryLockSlot,
};

struct TemporaryLocksQuota
{
    size_t quota = 0;
    TemporaryLocksQuotaReason reason = TemporaryLocksQuotaReason::Granted;
    /// The temporary locks must be taken through `TemporaryLockSlots`.
    bool needs_slot = false;
};

/// A consumer stops leaving the free topic partitions to a consumer below its quota after this many rounds, and falls
/// back to taking them every other round. Below its quota a consumer only waits for free topic partitions, so if it
/// cannot take them in this many rounds, it is probably not polling at all (e.g. stuck in a long insert into the views),
/// and the free topic partitions must not stay unconsumed because of it.
constexpr size_t MAX_ROUNDS_TO_YIELD_TO_CONSUMER_BELOW_QUOTA = 8;

TemporaryLocksQuota computeTemporaryLocksQuota(const TemporaryLocksQuotaInput & input);

}
