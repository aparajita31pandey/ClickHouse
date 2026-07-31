#include <gtest/gtest.h>
#include <config.h>

#if USE_RDKAFKA

#include <Storages/Kafka/StorageKafkaUtils.h>
#include <Common/Exception.h>

using namespace DB;
using StorageKafkaUtils::parseShardPartitions;

TEST(ParseShardPartitions, Basic)
{
    EXPECT_EQ(parseShardPartitions("0,1"), (std::vector<Int32>{0, 1}));
    EXPECT_EQ(parseShardPartitions("0,1,2,3"), (std::vector<Int32>{0, 1, 2, 3}));
}

TEST(ParseShardPartitions, SingleValue)
{
    EXPECT_EQ(parseShardPartitions("0"), (std::vector<Int32>{0}));
    EXPECT_EQ(parseShardPartitions("42"), (std::vector<Int32>{42}));
}

TEST(ParseShardPartitions, PreservesOrder)
{
    EXPECT_EQ(parseShardPartitions("3,0,2"), (std::vector<Int32>{3, 0, 2}));
}

TEST(ParseShardPartitions, Whitespace)
{
    EXPECT_EQ(parseShardPartitions(" 2 , 3 "), (std::vector<Int32>{2, 3}));
    EXPECT_EQ(parseShardPartitions("  7  "), (std::vector<Int32>{7}));
}

TEST(ParseShardPartitions, EmptyStringYieldsEmptyList)
{
    EXPECT_TRUE(parseShardPartitions("").empty());
    EXPECT_TRUE(parseShardPartitions("   ").empty());
}

TEST(ParseShardPartitions, RejectsNonNumeric)
{
    EXPECT_THROW(parseShardPartitions("x"), Exception);
    EXPECT_THROW(parseShardPartitions("0,x"), Exception);
}

TEST(ParseShardPartitions, RejectsTrailingJunk)
{
    EXPECT_THROW(parseShardPartitions("1x"), Exception);
    EXPECT_THROW(parseShardPartitions("0,1 2"), Exception);
}

TEST(ParseShardPartitions, RejectsNegative)
{
    EXPECT_THROW(parseShardPartitions("-1"), Exception);
    EXPECT_THROW(parseShardPartitions("0,-2"), Exception);
}

TEST(ParseShardPartitions, RejectsDuplicates)
{
    EXPECT_THROW(parseShardPartitions("0,0"), Exception);
    EXPECT_THROW(parseShardPartitions("0,1,0"), Exception);
}

TEST(ParseShardPartitions, RejectsEmptyToken)
{
    EXPECT_THROW(parseShardPartitions("0,,1"), Exception);
    EXPECT_THROW(parseShardPartitions("0,"), Exception);
    EXPECT_THROW(parseShardPartitions(","), Exception);
}

#endif
