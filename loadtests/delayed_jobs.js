import http from "k6/http";
import { check } from "k6";

export const options = {
  scenarios: {
    delayed_jobs: {
      executor: "constant-arrival-rate",
      rate: 50,
      timeUnit: "1s",
      duration: "5m",
      preAllocatedVUs: 50,
      maxVUs: 200,
    },
  },
};

export default function () {
  const unique = `${__VU}-${__ITER}-${Date.now()}`;
  const runAt = new Date(Date.now() + 60_000).toISOString();

  const payload = JSON.stringify({
    type: "payment.process_succeeded",
    payload: {
      payment_id: `pay_delayed_${unique}`,
      order_id: `order_delayed_${unique}`,
      user_id: `user_delayed_${unique}`,
      amount: 1990,
      currency: "RUB"
    },
    run_at: runAt,
    max_attempts: 3,
    idempotency_key: `loadtest:delayed:${unique}`
  });

  const res = http.post("http://localhost:8081/jobs", payload, {
    headers: {
      "Content-Type": "application/json",
    },
  });

  check(res, {
    "status is 200 or 201": (r) => r.status === 200 || r.status === 201,
  });
}
