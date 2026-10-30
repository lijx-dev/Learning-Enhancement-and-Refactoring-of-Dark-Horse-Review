import test from "node:test";
import assert from "node:assert/strict";
import {
  cacheSummary,
  readSource,
  receiptsComplete,
  receiptCount,
  sameOrder,
} from "../scripts/experience-helpers.js";

test("cache summary counts observed hits and loads without inventing unvisited layers", () => {
  const samples = [
    {
      reads: [
        {
          events: [
            { layer: "jvm", result: "miss" },
            { layer: "mysql", result: "load" },
          ],
        },
        { events: [{ layer: "scope", result: "hit" }] },
      ],
    },
  ];
  assert.deepEqual(cacheSummary(samples), {
    requests: 1,
    reads: 2,
    scopeHits: 1,
    mysqlLoads: 1,
  });
  assert.equal(readSource(samples[0].reads[0]), "MySQL");
  assert.equal(
    readSource({ events: [{ layer: "redis", result: "miss" }] }),
    "未采样",
  );
});

test("Kafka success requires every requested event persisted for the selected shop", () => {
  const rows = [
    { eventId: "a", shopId: 1, state: "PERSISTED" },
    { eventId: "b", shopId: 1, state: "PENDING" },
    { eventId: "unrequested", shopId: 1, state: "PERSISTED" },
  ];
  assert.equal(receiptsComplete(["a", "b"], rows, 1), false);
  assert.equal(receiptCount(["a", "b"], rows, 1), 1);
  rows[1].state = "PERSISTED";
  assert.equal(receiptsComplete(["a", "b"], rows, "1"), true);
  assert.equal(receiptsComplete(["a", "b"], rows, 2), false);
});

test("empty or duplicated event IDs cannot appear as a successful batch", () => {
  assert.equal(receiptsComplete([], [], 1), false);
  const row = { eventId: "a", shopId: 1, state: "PERSISTED" };
  assert.equal(receiptsComplete(["a", "a"], [row, row], 1), false);
  assert.equal(receiptCount(["a"], [row, row], 1), 1);
});

test("idempotency compares exact Java Long IDs rather than rounded numbers", () => {
  assert.equal(sameOrder("339587467398192145", "339587467398192145"), true);
  assert.equal(sameOrder("339587467398192145", "339587467398192146"), false);
  assert.equal(sameOrder(null, null), false);
});
