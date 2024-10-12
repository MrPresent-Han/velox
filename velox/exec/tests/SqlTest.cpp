/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#include "velox/exec/tests/utils/AssertQueryBuilder.h"
#include "velox/exec/tests/utils/OperatorTestBase.h"
#include "velox/parse/QueryPlanner.h"
#include "velox/exec/Task.h"
#include "velox/core/PlanFragment.h"
#include "velox/exec/PlanNodeStats.h"

namespace facebook::velox::exec::test {

class SqlTest : public OperatorTestBase {
 protected:
  void TearDown() override {
    planner_.reset();
    OperatorTestBase::TearDown();
  }

  void assertSql(const std::string& sql, const std::string& duckSql = "") {
    auto plan = planner_->plan(sql);
    AssertQueryBuilder(plan, duckDbQueryRunner_)
        .assertResults(duckSql.empty() ? sql : duckSql);
  }

  std::unique_ptr<core::DuckDbQueryPlanner> planner_{
      std::make_unique<core::DuckDbQueryPlanner>(pool())};
};

TEST_F(SqlTest, values) {
  assertSql("SELECT x, x + 5 FROM UNNEST([1, 2, 3]) as t(x)");
  assertSql("SELECT avg(x), count(*) FROM UNNEST([1, 2, 3]) as t(x)");
  assertSql("SELECT x % 5, avg(x) FROM UNNEST([1, 2, 3]) as t(x) GROUP BY 1");
  assertSql("SELECT avg(x * 4) FROM UNNEST([1, 2, 3]) as t(x)");
  assertSql("SELECT x / 5, avg(x * 4) FROM UNNEST([1, 2, 3]) as t(x) GROUP BY 1");
}

TEST_F(SqlTest, hnew) {
  std::cout << "starting sqlTest" << std::endl;
  auto sql = "SELECT avg(x), count(*) FROM UNNEST([1, 2, 3, 100]) as t(x)";
  auto plan = planner_->plan(sql);
  const facebook::velox::core::PlanFragment plan_fragment(plan);
  auto task = Task::create(
      "single.execution.task.0",
      plan_fragment,
      0,
      core::QueryCtx::create(),
      Task::ExecutionMode::kSerial);

  VELOX_CHECK(task->supportsSingleThreadedExecution());

  vector_size_t numRows = 0;
  std::vector<RowVectorPtr> results;
  int idx = 0;
  for (;;) {
    auto result = task->next();
    if (!result) {
      break;
    }
    std::cout << "hc===next_result:" << result.get()->toString() << ";" << idx << std::endl;
    idx+=1;

    for (auto& child : result->children()) {
      child->loadedVector();
    }
    results.push_back(result);
    numRows += result->size();
  }

  VELOX_CHECK(waitForTaskCompletion(task.get()));

  auto planNodeStats = toPlanStats(task->taskStats());
  VELOX_CHECK(planNodeStats.count(plan_fragment.planNode->id()));
  VELOX_CHECK_EQ(numRows, planNodeStats.at(plan_fragment.planNode->id()).outputRows);
  VELOX_CHECK_EQ(
      results.size(), planNodeStats.at(plan_fragment.planNode->id()).outputVectors);
  std::cout << "hc===results.size:" << results.size() << ", num_rows:" << numRows
      << ", result_string" << results[0]->toString() << std::endl;
  std::cout << "hc==res:" << results[0]->toString(0, results[0]->size()) << std::endl;
}

TEST_F(SqlTest, customScalarFunctions) {
  planner_->registerScalarFunction(
      "array_join", {ARRAY(BIGINT()), VARCHAR()}, VARCHAR());

  assertSql("SELECT array_join([1, 2, 3], '-')", "SELECT '1-2-3'");
}

TEST_F(SqlTest, customAggregateFunctions) {
  // We need an aggregate that DuckDB does not support. 'every' fits the need.
  // 'every' is an alias for bool_and().
  planner_->registerAggregateFunction("every", {BOOLEAN()}, BOOLEAN());

  assertSql(
      "SELECT every(x) FROM UNNEST([true, false, true]) as t(x)",
      "SELECT false");
  assertSql(
      "SELECT x, every(x) FROM UNNEST([true, false, true, false]) as t(x) GROUP BY 1",
      "VALUES (true, true), (false, false)");
}

TEST_F(SqlTest, tableScan) {
  std::unordered_map<std::string, std::vector<RowVectorPtr>> data = {
      {"t",
       {makeRowVector(
           {"a", "b", "c"},
           {
               makeFlatVector<int64_t>({1, 2, 3, 1, 2, 3}),
               makeFlatVector<int32_t>({1, 2, 3, 4, 5, 6}),
               makeFlatVector<int16_t>({3, 6, 8, 2, 9, 10}),
           })}},
      {"u",
       {makeRowVector(
           {"a", "b"},
           {
               makeFlatVector<int64_t>({1, 2, 3, 4, 5, 1}),
               makeFlatVector<double>({1.1, 2.2, 3.3, 4.4, 5.5, 1.2}),
           })}},
  };

  createDuckDbTable("t", data.at("t"));
  createDuckDbTable("u", data.at("u"));

  planner_->registerTable("t", data.at("t"));
  planner_->registerTable("u", data.at("u"));

  //assertSql("SELECT a, avg(b) FROM t WHERE c > 5 GROUP BY 1");
  assertSql("SELECT * FROM t, u WHERE t.a = u.a");
  //assertSql("SELECT t.a, t.b, t.c, u.b FROM t, u WHERE t.a = u.a");
}

TEST_F(SqlTest, tableScanNew) {
  std::unordered_map<std::string, std::vector<RowVectorPtr>> data = {
      {"t",
       {makeRowVector(
           {"a", "b", "c"},
           {
               makeFlatVector<int64_t>({1, 2, 3, 1, 2, 3}),
               makeFlatVector<int32_t>({1, 2, 3, 4, 5, 6}),
               makeFlatVector<int16_t>({3, 6, 8, 2, 9, 10}),
           })}},
      {"u",
       {makeRowVector(
           {"a", "b"},
           {
               makeFlatVector<int64_t>({1, 2, 3, 4, 5, 1}),
               makeFlatVector<double>({1.1, 2.2, 3.3, 4.4, 5.5, 1.2}),
           })}},
  };

  createDuckDbTable("t", data.at("t"));
  createDuckDbTable("u", data.at("u"));

  planner_->registerTable("t", data.at("t"));
  planner_->registerTable("u", data.at("u"));

  //assertSql("SELECT a, avg(b) FROM t WHERE c > 5 GROUP BY 1");
  //assertSql("SELECT t.a, t.b, t.c, u.b FROM t, u WHERE t.a = u.a");
  //auto sql = "SELECT * FROM t, u WHERE t.a = u.a";
  //auto sql = "SELECT * FROM t";
  //auto sql = "SELECT a, COUNT(a) FROM t GROUP BY a;";
  //auto sql = "SELECT * FROM t WHERE (c > 5 AND b <= 2)";
  //auto sql = "SELECT a, COUNT(b) FROM t WHERE c > 5 GROUP BY a HAVING COUNT(*) > 2";
  auto sql = "SELECT a FROM t GROUP BY a HAVING SUM(c) > 3.0";
  //auto sql = "SELECT a, SUM(b) FROM t GROUP BY a";
  //auto sql = "SELECT * FROM t ORDER BY a";

  auto plan = planner_->plan(sql);
  const facebook::velox::core::PlanFragment plan_fragment(plan);
  auto task = Task::create(
      "single.execution.task.0",
      plan_fragment,
      0,
      core::QueryCtx::create(),
      Task::ExecutionMode::kSerial);

  VELOX_CHECK(task->supportsSingleThreadedExecution());

  vector_size_t numRows = 0;
  std::vector<RowVectorPtr> results;
  int idx = 0;
  for (;;) {
    auto result = task->next();
    if (!result) {
      break;
    }
    std::cout << "hc===next_result:" << result.get()->toString() << ";" << idx << std::endl;
    idx+=1;

    for (auto& child : result->children()) {
      child->loadedVector();
    }
    results.push_back(result);
    numRows += result->size();
  }
  std::cout << "hc===results.size:" << results.size() << ", num_rows:" << numRows << std::endl;
  /*std::cout << "hc===results.size:" << results.size() << ", num_rows:" << numRows
            << ", result_string" << results[0]->toString() << std::endl;*/
}
} // namespace facebook::velox::exec::test
