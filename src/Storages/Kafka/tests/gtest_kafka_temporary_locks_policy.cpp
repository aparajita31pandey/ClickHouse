#include <gtest/gtest.h>
#include <config.h>

#if USE_RDKAFKA

#include <Storages/Kafka/KafkaTemporaryLocksPolicy.h>

#include <algorithm>
#include <map>
#include <set>
#include <thread>

using namespace DB;

namespace
{

using enum TemporaryLocksQuotaReason;

/// A consumer of a converged cluster where a leftover topic partition is free: nothing prevents taking it.
TemporaryLocksQuotaInput leftoverIsFree()
{
    return TemporaryLocksQuotaInput{
        .previous_quota = 0,
        .available_topic_partitions = 1,
        .has_replica_without_locks = false,
        .has_consumer_below_quota = false,
        .rounds_with_consumer_below_quota = 0,
        .is_least_loaded_consumer = true,
    };
}

}

// ---------------------------------------------------------------------------
// Quotas
// ---------------------------------------------------------------------------

TEST(KafkaTemporaryLocksPolicy, NodeQuota)
{
    EXPECT_EQ(computeNodeQuota(64, 3), 21U);
    EXPECT_EQ(computeNodeQuota(64, 2), 32U);
    EXPECT_EQ(computeNodeQuota(2, 3), 1U) << "every replica gets at least one topic partition";
    EXPECT_EQ(computeNodeQuota(64, 0), 64U) << "a missing own is_active must not divide by zero";
}

TEST(KafkaTemporaryLocksPolicy, ConsumerQuotasSumUpToTheNodeQuota)
{
    for (const size_t node_quota : {1U, 21U, 22U, 32U, 64U})
    {
        for (const size_t num_consumers : {1U, 7U, 22U})
        {
            size_t sum = 0;
            for (size_t idx = 0; idx < num_consumers; ++idx)
                sum += computeConsumerQuota(node_quota, num_consumers, idx);
            EXPECT_EQ(sum, node_quota) << "node_quota=" << node_quota << ", num_consumers=" << num_consumers;
        }
    }
    EXPECT_EQ(computeConsumerQuota(32, 22, 0), 2U);
    EXPECT_EQ(computeConsumerQuota(32, 22, 9), 2U);
    EXPECT_EQ(computeConsumerQuota(32, 22, 10), 1U);
    EXPECT_EQ(computeConsumerQuota(21, 22, 21), 0U) << "more consumers than topic partitions";
}

// ---------------------------------------------------------------------------
// summarizeReplicaLocks
// ---------------------------------------------------------------------------

TEST(KafkaTemporaryLocksPolicy, SummaryOfConvergedClusterWithLeftover)
{
    const auto summary = summarizeReplicaLocks({"a", "b", "c"}, {{"a", 22}, {"b", 21}, {"c", 21}}, "a", 64);
    EXPECT_EQ(summary.active_replica_count, 3U);
    EXPECT_FALSE(summary.has_replica_without_locks);
    EXPECT_FALSE(summary.has_other_replica_below_quota);
}

TEST(KafkaTemporaryLocksPolicy, SummaryDetectsOtherReplicaBelowQuotaAfterFailover)
{
    /// The production case after losing one of three replicas: 33 + 31 instead of 32 + 32.
    const std::unordered_map<String, size_t> locks{{"bubij", 33}, {"bunas", 31}};

    const auto from_bubij = summarizeReplicaLocks({"bubij", "bunas"}, locks, "bubij", 64);
    EXPECT_EQ(from_bubij.active_replica_count, 2U);
    EXPECT_TRUE(from_bubij.has_other_replica_below_quota);

    const auto from_bunas = summarizeReplicaLocks({"bubij", "bunas"}, locks, "bunas", 64);
    EXPECT_FALSE(from_bunas.has_other_replica_below_quota) << "bubij is above its quota, not below";
}

TEST(KafkaTemporaryLocksPolicy, SummaryTreatsReplicaWithoutLocksSeparately)
{
    const auto summary = summarizeReplicaLocks({"a", "b", "c"}, {{"a", 32}, {"b", 32}}, "a", 64);
    EXPECT_TRUE(summary.has_replica_without_locks);
    EXPECT_FALSE(summary.has_other_replica_below_quota);
}

TEST(KafkaTemporaryLocksPolicy, SummaryIgnoresLocksOfInactiveReplicas)
{
    const auto summary = summarizeReplicaLocks({"a", "b"}, {{"a", 32}, {"b", 31}, {"dead", 1}}, "a", 64);
    EXPECT_EQ(summary.active_replica_count, 2U);
    EXPECT_FALSE(summary.has_replica_without_locks);
    EXPECT_TRUE(summary.has_other_replica_below_quota);
}

TEST(KafkaTemporaryLocksPolicy, SummaryNeverReportsOwnReplicaBelowQuota)
{
    /// The own replica may be below its quota because some of its consumers could not be created.
    const auto summary = summarizeReplicaLocks({"a", "b"}, {{"a", 20}, {"b", 33}}, "a", 64);
    EXPECT_FALSE(summary.has_other_replica_below_quota);
}

TEST(KafkaTemporaryLocksPolicy, SummaryClampsActiveReplicaCount)
{
    const auto summary = summarizeReplicaLocks({}, {{"a", 3}}, "a", 64);
    EXPECT_EQ(summary.active_replica_count, 1U);
    EXPECT_FALSE(summary.has_replica_without_locks);
}

TEST(KafkaTemporaryLocksPolicy, SummaryOfSmallTopic)
{
    /// 2 topic partitions over 3 replicas: the quota is 1 and one replica stays without locks.
    const auto summary = summarizeReplicaLocks({"a", "b", "c"}, {{"a", 1}, {"b", 1}}, "a", 2);
    EXPECT_TRUE(summary.has_replica_without_locks);
    EXPECT_FALSE(summary.has_other_replica_below_quota);
}

// ---------------------------------------------------------------------------
// TemporaryLockSlots
// ---------------------------------------------------------------------------

TEST(KafkaTemporaryLocksPolicy, SlotsAreLimited)
{
    auto slots = std::make_shared<TemporaryLockSlots>();
    auto first = slots->tryAcquire(1);
    ASSERT_TRUE(first.has_value());
    EXPECT_FALSE(slots->tryAcquire(1).has_value());
    EXPECT_EQ(slots->used(), 1U);

    auto second = slots->tryAcquire(2);
    EXPECT_TRUE(second.has_value()) << "the limit is checked per call";
    EXPECT_FALSE(slots->tryAcquire(2).has_value());
    EXPECT_FALSE(slots->tryAcquire(0).has_value());
}

TEST(KafkaTemporaryLocksPolicy, SlotIsReleasedWhenDestroyed)
{
    auto slots = std::make_shared<TemporaryLockSlots>();
    {
        auto slot = slots->tryAcquire(1);
        ASSERT_TRUE(slot.has_value());
    }
    EXPECT_EQ(slots->used(), 0U);
    EXPECT_TRUE(slots->tryAcquire(1).has_value());
}

TEST(KafkaTemporaryLocksPolicy, MovedSlotIsReleasedOnce)
{
    auto slots = std::make_shared<TemporaryLockSlots>();
    std::optional<TemporaryLockSlots::Slot> owner;
    {
        auto slot = slots->tryAcquire(1);
        ASSERT_TRUE(slot.has_value());
        owner = std::move(slot);
    }
    EXPECT_EQ(slots->used(), 1U) << "the moved-from slot must not release";

    auto other = slots->tryAcquire(2);
    ASSERT_TRUE(other.has_value());
    EXPECT_EQ(slots->used(), 2U);
    *owner = std::move(*other);
    EXPECT_EQ(slots->used(), 1U) << "the assigned-to slot releases what it held";

    owner.reset();
    EXPECT_EQ(slots->used(), 0U);
}

TEST(KafkaTemporaryLocksPolicy, SlotOutlivesTheRegistry)
{
    std::optional<TemporaryLockSlots::Slot> slot;
    {
        auto registry = std::make_shared<ConsumerLocksRegistry>(1);
        registry->addConsumer(0);
        slot = registry->tryAcquireTemporaryLockSlot(1);
        ASSERT_TRUE(slot.has_value());
    }
    /// Releasing must not touch freed memory, checked by the sanitizer builds.
    slot.reset();
}

TEST(KafkaTemporaryLocksPolicy, ConcurrentConsumersNeverExceedTheLimit)
{
    /// The race this replaces: two consumers that read the lock count from Keeper at the same moment both took the last
    /// temporary lock of the replica.
    auto slots = std::make_shared<TemporaryLockSlots>();
    constexpr size_t limit = 1;
    std::atomic<size_t> holders{0};
    std::atomic<size_t> max_holders{0};
    std::atomic<size_t> acquired{0};

    std::vector<std::thread> threads;
    for (size_t t = 0; t < 22; ++t)
    {
        threads.emplace_back(
            [&]
            {
                for (size_t i = 0; i < 20000; ++i)
                {
                    auto slot = slots->tryAcquire(limit);
                    if (!slot)
                        continue;
                    ++acquired;
                    const size_t now = ++holders;
                    size_t seen = max_holders.load();
                    while (now > seen && !max_holders.compare_exchange_weak(seen, now))
                        ;
                    --holders;
                }
            });
    }
    for (auto & thread : threads)
        thread.join();

    EXPECT_GT(acquired.load(), 0U);
    EXPECT_LE(max_holders.load(), limit);
    EXPECT_EQ(slots->used(), 0U);
}

// ---------------------------------------------------------------------------
// ConsumerLocksRegistry
// ---------------------------------------------------------------------------

TEST(KafkaTemporaryLocksPolicy, RegistryReportsSiblingBelowQuota)
{
    ConsumerLocksRegistry registry(3);
    for (size_t idx = 0; idx < 3; ++idx)
        registry.addConsumer(idx);
    EXPECT_FALSE(registry.hasOtherConsumerBelowQuota(0)) << "nothing is known before the first report";

    registry.reportPermanentLocks(0, 2, 2);
    registry.reportPermanentLocks(1, 1, 2);
    EXPECT_TRUE(registry.hasOtherConsumerBelowQuota(0));
    EXPECT_FALSE(registry.hasOtherConsumerBelowQuota(1)) << "a consumer does not wait for itself";

    registry.reportPermanentLocks(1, 2, 2);
    EXPECT_FALSE(registry.hasOtherConsumerBelowQuota(0));
}

TEST(KafkaTemporaryLocksPolicy, RegistryForgetsLocksOfReplacedSessionAndDestroyedConsumers)
{
    ConsumerLocksRegistry registry(3);
    for (size_t idx = 0; idx < 3; ++idx)
        registry.addConsumer(idx);

    registry.reportPermanentLocks(1, 0, 2);
    ASSERT_TRUE(registry.hasOtherConsumerBelowQuota(0));
    registry.forgetPermanentLocks(1);
    EXPECT_FALSE(registry.hasOtherConsumerBelowQuota(0)) << "it reports again after its next round";

    registry.reportPermanentLocks(2, 0, 2);
    ASSERT_TRUE(registry.hasOtherConsumerBelowQuota(0));
    registry.removeConsumer(2);
    EXPECT_FALSE(registry.hasOtherConsumerBelowQuota(0));

    registry.reportPermanentLocks(2, 0, 2);
    EXPECT_FALSE(registry.hasOtherConsumerBelowQuota(0)) << "reports of destroyed consumers are ignored";
    registry.reportPermanentLocks(7, 0, 2);
    EXPECT_FALSE(registry.hasOtherConsumerBelowQuota(0)) << "reports of unknown consumers are ignored";
}

TEST(KafkaTemporaryLocksPolicy, RegistryAllowsOneTemporaryLockPerReplica)
{
    ConsumerLocksRegistry registry(22);
    for (size_t idx = 0; idx < 22; ++idx)
        registry.addConsumer(idx);
    EXPECT_EQ(registry.getTemporaryLockSlotsLimit(21), 1U);
    EXPECT_EQ(registry.getTemporaryLockSlotsLimit(32), 1U);

    auto slot = registry.tryAcquireTemporaryLockSlot(21);
    ASSERT_TRUE(slot.has_value());
    EXPECT_FALSE(registry.tryAcquireTemporaryLockSlot(21).has_value());
    EXPECT_EQ(registry.getUsedTemporaryLockSlots(), 1U);
}

TEST(KafkaTemporaryLocksPolicy, RegistryCoversTheQuotaOfConsumersThatCouldNotBeCreated)
{
    /// Consumers 0-2 of 22 could not be created. With a node quota of 32 they would hold 2 + 2 + 2 permanent locks, so
    /// the replica may hold 7 temporary locks: 26 permanent + 7 temporary = 33 = node_quota + 1.
    ConsumerLocksRegistry registry(22);
    for (size_t idx = 3; idx < 22; ++idx)
        registry.addConsumer(idx);
    EXPECT_EQ(registry.getTemporaryLockSlotsLimit(32), 7U);

    /// With 21 topic partitions per replica, consumers 0-20 have a quota of 1.
    EXPECT_EQ(registry.getTemporaryLockSlotsLimit(21), 4U);

    /// A destroyed consumer leaves its quota unused as well.
    registry.removeConsumer(3);
    EXPECT_EQ(registry.getTemporaryLockSlotsLimit(32), 9U);
}

TEST(KafkaTemporaryLocksPolicy, RegistryPicksTheConsumersWithTheSmallestQuota)
{
    /// 32 topic partitions over 22 consumers: consumers 0-9 hold 2, consumers 10-21 hold 1.
    ConsumerLocksRegistry registry(22);
    for (size_t idx = 0; idx < 22; ++idx)
        registry.addConsumer(idx);
    EXPECT_FALSE(registry.isLeastLoadedConsumer(0, 32));
    EXPECT_FALSE(registry.isLeastLoadedConsumer(9, 32));
    EXPECT_TRUE(registry.isLeastLoadedConsumer(10, 32));
    EXPECT_TRUE(registry.isLeastLoadedConsumer(21, 32));

    /// 21 topic partitions over 22 consumers: only consumer 21 is idle.
    EXPECT_FALSE(registry.isLeastLoadedConsumer(20, 21));
    EXPECT_TRUE(registry.isLeastLoadedConsumer(21, 21));

    /// An even split: everybody qualifies.
    EXPECT_TRUE(registry.isLeastLoadedConsumer(0, 44));
}

TEST(KafkaTemporaryLocksPolicy, RegistryLetsEveryConsumerCoverConsumersThatCouldNotBeCreated)
{
    /// Consumers 0-2 could not be created and their 3 topic partitions need 3 temporary locks, but only consumer 21 has
    /// the smallest quota. Every consumer qualifies, so the replica can actually use its 4 slots.
    ConsumerLocksRegistry registry(22);
    for (size_t idx = 3; idx < 22; ++idx)
        registry.addConsumer(idx);
    EXPECT_TRUE(registry.isLeastLoadedConsumer(3, 21));
    EXPECT_TRUE(registry.isLeastLoadedConsumer(21, 21));

    /// The failed consumers have no quota when there are fewer topic partitions than consumers, nothing to cover.
    ConsumerLocksRegistry small(4);
    for (size_t idx = 0; idx < 3; ++idx)
        small.addConsumer(idx);
    EXPECT_EQ(small.getTemporaryLockSlotsLimit(2), 1U);
    EXPECT_FALSE(small.isLeastLoadedConsumer(0, 2));
    EXPECT_TRUE(small.isLeastLoadedConsumer(2, 2));
}

// ---------------------------------------------------------------------------
// computeTemporaryLocksQuota
// ---------------------------------------------------------------------------

TEST(KafkaTemporaryLocksPolicy, GrantsALeftoverThroughASlot)
{
    const auto decision = computeTemporaryLocksQuota(leftoverIsFree());
    EXPECT_EQ(decision.quota, 1U);
    EXPECT_EQ(decision.reason, Granted);
    EXPECT_TRUE(decision.needs_slot);
}

TEST(KafkaTemporaryLocksPolicy, KeepsTheLeftoverAcrossRounds)
{
    auto input = leftoverIsFree();
    input.previous_quota = 1;
    const auto decision = computeTemporaryLocksQuota(input);
    EXPECT_EQ(decision.quota, 1U) << "no 1, 0, 1, 0 flapping in a healthy cluster";
}

TEST(KafkaTemporaryLocksPolicy, NeverMoreThanOneTemporaryLockPerConsumer)
{
    /// Before, the quota grew by one per round up to the number of free topic partitions.
    auto input = leftoverIsFree();
    input.available_topic_partitions = 10;
    for (size_t round = 0; round < 10; ++round)
    {
        const auto decision = computeTemporaryLocksQuota(input);
        EXPECT_LE(decision.quota, 1U) << "round " << round;
        input.previous_quota = decision.quota;
    }
}

TEST(KafkaTemporaryLocksPolicy, NothingToLock)
{
    auto input = leftoverIsFree();
    input.available_topic_partitions = 0;
    input.previous_quota = 1;
    const auto decision = computeTemporaryLocksQuota(input);
    EXPECT_EQ(decision.quota, 0U);
    EXPECT_EQ(decision.reason, NoAvailableTopicPartitions);
}

TEST(KafkaTemporaryLocksPolicy, YieldsToConsumerBelowQuota)
{
    auto input = leftoverIsFree();
    input.previous_quota = 1;
    input.has_consumer_below_quota = true;
    for (size_t round = 1; round <= MAX_ROUNDS_TO_YIELD_TO_CONSUMER_BELOW_QUOTA; ++round)
    {
        input.rounds_with_consumer_below_quota = round;
        const auto decision = computeTemporaryLocksQuota(input);
        EXPECT_EQ(decision.quota, 0U) << "round " << round;
        EXPECT_EQ(decision.reason, YieldToConsumerBelowQuota);
        input.previous_quota = decision.quota;
    }
}

TEST(KafkaTemporaryLocksPolicy, StopsYieldingToAConsumerThatDoesNotTakeTheTopicPartitions)
{
    /// A consumer stuck below its quota must not leave the free topic partitions unconsumed forever.
    auto input = leftoverIsFree();
    input.has_consumer_below_quota = true;
    input.rounds_with_consumer_below_quota = MAX_ROUNDS_TO_YIELD_TO_CONSUMER_BELOW_QUOTA;

    std::vector<size_t> quotas;
    for (size_t round = 0; round < 4; ++round)
    {
        ++input.rounds_with_consumer_below_quota;
        const auto decision = computeTemporaryLocksQuota(input);
        quotas.push_back(decision.quota);
        input.previous_quota = decision.quota;
    }
    EXPECT_EQ(quotas, (std::vector<size_t>{1, 0, 1, 0})) << "every other round, so the waiting consumer still gets chances";
}

TEST(KafkaTemporaryLocksPolicy, CoversReplicaWithoutLocksEveryOtherRoundWithoutSlot)
{
    /// Exactly the behaviour from before: a replica that does not consume is covered every other round.
    auto input = leftoverIsFree();
    input.available_topic_partitions = 22;
    input.has_replica_without_locks = true;

    std::vector<size_t> quotas;
    for (size_t round = 0; round < 4; ++round)
    {
        const auto decision = computeTemporaryLocksQuota(input);
        quotas.push_back(decision.quota);
        if (decision.quota > 0)
        {
            EXPECT_EQ(decision.reason, CoverReplicaWithoutLocks);
            EXPECT_FALSE(decision.needs_slot) << "covering must not be limited to one temporary lock per replica";
        }
        input.previous_quota = decision.quota;
    }
    EXPECT_EQ(quotas, (std::vector<size_t>{1, 0, 1, 0}));
}

TEST(KafkaTemporaryLocksPolicy, LeavesTheLeftoverToTheLeastLoadedConsumer)
{
    auto input = leftoverIsFree();
    input.is_least_loaded_consumer = false;
    const auto decision = computeTemporaryLocksQuota(input);
    EXPECT_EQ(decision.quota, 0U);
    EXPECT_EQ(decision.reason, NotLeastLoadedConsumer);

    /// Covering a replica without locks is not restricted, every consumer helps.
    input.has_replica_without_locks = true;
    EXPECT_EQ(computeTemporaryLocksQuota(input).quota, 1U);
}

TEST(KafkaTemporaryLocksPolicy, YieldsBeforeCoveringReplicaWithoutLocks)
{
    auto input = leftoverIsFree();
    input.has_replica_without_locks = true;
    input.has_consumer_below_quota = true;
    input.rounds_with_consumer_below_quota = 1;
    EXPECT_EQ(computeTemporaryLocksQuota(input).reason, YieldToConsumerBelowQuota);
}

// ---------------------------------------------------------------------------
// The policy in rounds: an in-memory model of `KeeperHandlingConsumer::prepareToPoll`.
//
// Every consumer of every replica runs a round in turn: it drops its temporary locks, updates its permanent locks
// exactly like `updatePermanentLocksLocked`, and asks the policy for temporary locks, taking them through the slots of
// its replica. Keeper is replaced by a map from topic partition to holder. The order of the rounds is fixed, so a
// consumer that releases a temporary lock always gets the chance to take it back before anybody else, which is the
// worst case for the consumer that waits for it.
// ---------------------------------------------------------------------------

namespace
{

struct Model
{
    struct Consumer
    {
        size_t replica = 0;
        size_t idx = 0;
        bool exists = true;
        bool stuck = false;
        std::set<size_t> permanent{};
        std::set<size_t> temporary{};
        std::vector<TemporaryLockSlots::Slot> slots{};
        size_t tmp_quota = 0;
        size_t rounds_below = 0;
    };

    size_t partitions = 0;
    size_t num_consumers = 0;
    std::vector<String> replica_names;
    std::vector<bool> active;
    std::vector<bool> consuming;
    std::vector<Consumer> consumers;
    std::vector<ConsumerLocksRegistryPtr> registries;
    std::map<size_t, size_t> holder; /// topic partition -> index in `consumers`
    /// A consumer covered a replica without locks, which is not limited by the slots, in the current or in the previous
    /// round of everyone. Such a temporary lock is dropped in the consumer's next round.
    bool covered_in_this_round = false;
    bool covered_in_previous_round = false;

    Model(size_t partitions_, size_t num_replicas, size_t num_consumers_)
        : partitions(partitions_), num_consumers(num_consumers_), active(num_replicas, true), consuming(num_replicas, true)
    {
        for (size_t r = 0; r < num_replicas; ++r)
        {
            replica_names.push_back("r" + std::to_string(r));
            registries.push_back(std::make_shared<ConsumerLocksRegistry>(num_consumers));
            for (size_t i = 0; i < num_consumers; ++i)
            {
                registries[r]->addConsumer(i);
                consumers.push_back(Consumer{
                    .replica = r,
                    .idx = i,
                    .exists = true,
                    .stuck = false,
                    .permanent = {},
                    .temporary = {},
                    .slots = {},
                    .tmp_quota = 0,
                    .rounds_below = 0});
            }
        }
    }

    /// The consumer could not be created in `StorageKafka2::startup`.
    void failToCreate(size_t replica, size_t idx)
    {
        consumers[replica * num_consumers + idx].exists = false;
        registries[replica]->removeConsumer(idx);
    }

    std::vector<String> activeReplicas() const
    {
        std::vector<String> result;
        for (size_t r = 0; r < active.size(); ++r)
            if (active[r])
                result.push_back(replica_names[r]);
        return result;
    }

    size_t nodeQuota() const { return computeNodeQuota(partitions, activeReplicas().size()); }

    std::unordered_map<String, size_t> lockCountByReplica() const
    {
        std::unordered_map<String, size_t> result;
        for (const auto & [p, h] : holder)
            ++result[replica_names[consumers[h].replica]];
        return result;
    }

    void dropTemporaryLocks(Consumer & consumer)
    {
        for (auto p : consumer.temporary)
            holder.erase(p);
        consumer.temporary.clear();
        consumer.slots.clear();
    }

    void kill(size_t replica)
    {
        active[replica] = false;
        for (auto & consumer : consumers)
        {
            if (consumer.replica != replica)
                continue;
            dropTemporaryLocks(consumer);
            for (auto p : consumer.permanent)
                holder.erase(p);
            consumer.permanent.clear();
            /// The ephemeral nodes are gone with the session, the consumer starts from scratch with a new one.
            registries[replica]->forgetPermanentLocks(consumer.idx);
            consumer.tmp_quota = 0;
            consumer.rounds_below = 0;
        }
    }

    void round(size_t c)
    {
        auto & consumer = consumers[c];
        if (!consumer.exists || consumer.stuck || !active[consumer.replica] || !consuming[consumer.replica])
            return;

        dropTemporaryLocks(consumer);
        const auto summary = summarizeReplicaLocks(activeReplicas(), lockCountByReplica(), replica_names[consumer.replica], partitions);

        /// updatePermanentLocksLocked
        const size_t node_quota = computeNodeQuota(partitions, summary.active_replica_count);
        const size_t quota = computeConsumerQuota(node_quota, num_consumers, consumer.idx);
        while (consumer.permanent.size() > quota)
        {
            holder.erase(*consumer.permanent.begin());
            consumer.permanent.erase(consumer.permanent.begin());
        }
        for (size_t p = 0; p < partitions && consumer.permanent.size() < quota; ++p)
        {
            if (!holder.contains(p))
            {
                holder[p] = c;
                consumer.permanent.insert(p);
            }
        }

        /// lockTemporaryLocksLocked
        auto & registry = *registries[consumer.replica];
        registry.reportPermanentLocks(consumer.idx, consumer.permanent.size(), quota);
        const bool below = summary.has_other_replica_below_quota || registry.hasOtherConsumerBelowQuota(consumer.idx);
        consumer.rounds_below = below ? consumer.rounds_below + 1 : 0;

        std::vector<size_t> free;
        for (size_t p = 0; p < partitions; ++p)
            if (!holder.contains(p))
                free.push_back(p);

        auto decision = computeTemporaryLocksQuota(TemporaryLocksQuotaInput{
            .previous_quota = consumer.tmp_quota,
            .available_topic_partitions = free.size(),
            .has_replica_without_locks = summary.has_replica_without_locks,
            .has_consumer_below_quota = below,
            .rounds_with_consumer_below_quota = consumer.rounds_below,
            .is_least_loaded_consumer = registry.isLeastLoadedConsumer(consumer.idx, node_quota),
        });
        std::optional<TemporaryLockSlots::Slot> slot;
        if (decision.needs_slot)
        {
            slot = registry.tryAcquireTemporaryLockSlot(node_quota);
            if (!slot)
                decision = {.quota = 0, .reason = NoTemporaryLockSlot, .needs_slot = false};
        }
        if (decision.reason == CoverReplicaWithoutLocks)
            covered_in_this_round = true;
        consumer.tmp_quota = decision.quota;
        for (size_t i = 0; i < decision.quota && i < free.size(); ++i)
        {
            holder[free[i]] = c;
            consumer.temporary.insert(free[i]);
            if (slot)
                consumer.slots.push_back(std::move(*slot));
        }
    }

    void roundOfEveryone()
    {
        covered_in_previous_round = covered_in_this_round;
        covered_in_this_round = false;
        for (size_t c = 0; c < consumers.size(); ++c)
            round(c);
    }

    /// Every existing consumer of a consuming replica holds exactly its permanent quota and at most one temporary lock.
    bool converged() const
    {
        const size_t node_quota = nodeQuota();
        for (const auto & consumer : consumers)
        {
            if (!consumer.exists || consumer.stuck || !active[consumer.replica] || !consuming[consumer.replica])
                continue;
            if (consumer.permanent.size() != computeConsumerQuota(node_quota, num_consumers, consumer.idx)
                || consumer.temporary.size() > 1)
                return false;
        }
        return true;
    }

    size_t maxConsumerLoad() const
    {
        size_t result = 0;
        for (const auto & consumer : consumers)
            result = std::max(result, consumer.permanent.size() + consumer.temporary.size());
        return result;
    }

    size_t replicaLocks(size_t replica) const
    {
        size_t result = 0;
        for (const auto & consumer : consumers)
            if (consumer.replica == replica)
                result += consumer.permanent.size() + consumer.temporary.size();
        return result;
    }

    size_t replicaTemporaryLocks(size_t replica) const
    {
        size_t result = 0;
        for (const auto & consumer : consumers)
            if (consumer.replica == replica)
                result += consumer.temporary.size();
        return result;
    }

    /// The invariant of the change: a replica holds at most `node_quota + 1` locks. It is relaxed only while a replica
    /// without locks is covered, until the covering consumers run their next round.
    void expectReplicasWithinQuotaPlusOne(const std::string & context) const
    {
        if (covered_in_this_round || covered_in_previous_round)
            return;
        for (size_t r = 0; r < active.size(); ++r)
            if (active[r])
                EXPECT_LE(replicaLocks(r), nodeQuota() + 1) << context << ", replica " << r;
    }
};

/// Runs rounds until the model converges, checking the invariant after every round. Returns the number of rounds.
size_t runUntilConverged(Model & model, size_t max_rounds = 30)
{
    size_t rounds = 0;
    while (!model.converged() && rounds < max_rounds)
    {
        model.roundOfEveryone();
        ++rounds;
        model.expectReplicasWithinQuotaPlusOne("round " + std::to_string(rounds));
    }
    return rounds;
}

}

TEST(KafkaTemporaryLocksPolicy, ModelFailoverOfOneReplicaOfThreeConverges)
{
    /// The production shard: 64 topic partitions, 3 replicas with 22 consumers each, then one replica is lost.
    Model model(64, 3, 22);
    runUntilConverged(model);
    ASSERT_TRUE(model.converged());
    EXPECT_EQ(model.holder.size(), 64U);

    model.kill(2);
    const size_t rounds = runUntilConverged(model);
    EXPECT_TRUE(model.converged()) << "the survivors did not converge to 32 + 32 in " << rounds << " rounds";
    EXPECT_LE(rounds, 3U);
    EXPECT_EQ(model.holder.size(), 64U) << "every topic partition must be locked";
    EXPECT_EQ(model.replicaLocks(0), 32U);
    EXPECT_EQ(model.replicaLocks(1), 32U);
    EXPECT_LE(model.maxConsumerLoad(), 2U) << "32 topic partitions over 22 consumers means at most 2 per consumer";
}

TEST(KafkaTemporaryLocksPolicy, ModelFailoverWithOneConsumerPerReplica)
{
    Model model(64, 3, 1);
    runUntilConverged(model);
    ASSERT_TRUE(model.converged());

    model.kill(0);
    const size_t rounds = runUntilConverged(model);
    EXPECT_TRUE(model.converged());
    EXPECT_LE(rounds, 3U);
    EXPECT_EQ(model.replicaLocks(1), 32U);
    EXPECT_EQ(model.replicaLocks(2), 32U);
}

TEST(KafkaTemporaryLocksPolicy, ModelSteadyStateKeepsOneTemporaryLockPerReplica)
{
    /// 64 topic partitions over 5 replicas x 7 consumers: 4 leftovers, at most one per replica.
    Model model(64, 5, 7);
    runUntilConverged(model);
    ASSERT_TRUE(model.converged());
    for (size_t i = 0; i < 10; ++i)
    {
        model.roundOfEveryone();
        model.expectReplicasWithinQuotaPlusOne("round " + std::to_string(i));
        for (size_t r = 0; r < 5; ++r)
            EXPECT_LE(model.replicaTemporaryLocks(r), 1U) << "round " << i << ", replica " << r;
        EXPECT_LE(model.maxConsumerLoad(), 2U) << "the leftover goes to a consumer that holds 1, round " << i;
    }
    EXPECT_TRUE(model.converged());
    EXPECT_EQ(model.holder.size(), 64U) << "every leftover is consumed";
}

TEST(KafkaTemporaryLocksPolicy, ModelProductionSteadyStateGivesTheLeftoverToAnIdleConsumer)
{
    /// 64 topic partitions over 3 replicas x 22 consumers: 21 per replica, so consumer 21 of every replica is idle and
    /// takes the leftover.
    Model model(64, 3, 22);
    runUntilConverged(model);
    ASSERT_TRUE(model.converged());
    for (size_t i = 0; i < 10; ++i)
    {
        model.roundOfEveryone();
        model.expectReplicasWithinQuotaPlusOne("round " + std::to_string(i));
        EXPECT_LE(model.maxConsumerLoad(), 1U) << "round " << i;
    }
    EXPECT_EQ(model.holder.size(), 64U);
}

TEST(KafkaTemporaryLocksPolicy, ModelCoversReplicaThatDoesNotConsume)
{
    /// A replica is active in Keeper but has no materialized view attached. Its 21 topic partitions must still be
    /// covered by the others with temporary locks, as before.
    Model model(64, 3, 22);
    model.consuming[2] = false;
    std::set<size_t> covered;
    for (size_t i = 0; i < 6; ++i)
    {
        model.roundOfEveryone();
        for (const auto & [p, _] : model.holder)
            covered.insert(p);
        EXPECT_LE(model.maxConsumerLoad(), 2U) << "round " << i;
    }
    EXPECT_EQ(covered.size(), 64U) << "every topic partition must be consumed at least every other round";
}

TEST(KafkaTemporaryLocksPolicy, ModelReplicaJoins)
{
    Model model(64, 3, 22);
    model.active[2] = false;
    runUntilConverged(model);
    ASSERT_TRUE(model.converged());

    model.active[2] = true;
    const size_t rounds = runUntilConverged(model);
    EXPECT_TRUE(model.converged());
    EXPECT_LE(rounds, 4U);
    EXPECT_EQ(model.holder.size(), 64U);
}

TEST(KafkaTemporaryLocksPolicy, ModelCoversQuotaOfConsumersThatCouldNotBeCreated)
{
    /// Consumers 0-2 of one replica could not be created, so their 3 permanent locks stay free. The replica covers them
    /// with temporary locks instead of leaving them unconsumed, and still holds at most node_quota + 1 = 22 locks.
    Model model(64, 3, 22);
    for (size_t idx = 0; idx < 3; ++idx)
        model.failToCreate(0, idx);
    for (size_t i = 0; i < 12; ++i)
    {
        model.roundOfEveryone();
        model.expectReplicasWithinQuotaPlusOne("round " + std::to_string(i));
    }
    EXPECT_EQ(model.holder.size(), 64U) << "every topic partition must be locked";
    EXPECT_LE(model.replicaLocks(0), 22U);
}

TEST(KafkaTemporaryLocksPolicy, ModelStuckConsumerDoesNotBlockTheLeftoversForever)
{
    /// A consumer lost its permanent lock and stopped running its rounds while below its quota (e.g. stuck in a long
    /// insert). The others yield to it for a bounded number of rounds and then cover the free topic partitions.
    Model model(64, 3, 22);
    runUntilConverged(model);
    ASSERT_TRUE(model.converged());

    auto & stuck = model.consumers[0];
    for (auto p : stuck.permanent)
        model.holder.erase(p);
    stuck.permanent.clear();
    model.registries[0]->reportPermanentLocks(0, 0, 1);
    stuck.stuck = true;

    size_t rounds = 0;
    while (model.holder.size() < 64 && rounds < 20)
    {
        model.roundOfEveryone();
        ++rounds;
    }
    EXPECT_EQ(model.holder.size(), 64U) << "the free topic partitions are covered again after " << rounds << " rounds";
    EXPECT_LE(rounds, MAX_ROUNDS_TO_YIELD_TO_CONSUMER_BELOW_QUOTA + 2);
}

#endif // USE_RDKAFKA
