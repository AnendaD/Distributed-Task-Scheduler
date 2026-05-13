import http from "k6/http";
import { check } from "k6";

export const options = {
  scenarios: {
    create_jobs: {
      executor: "constant-arrival-rate",
      rate: 100,
      timeUnit: "1s",
      duration: "10m",
      preAllocatedVUs: 100,
      maxVUs: 300,
    },
  },
};

export default function () {
  const unique = `${__VU}-${__ITER}-${Date.now()}`;

  const payload = JSON.stringify({
    type: "payment.process_succeeded",
    payload: {
      payment_id: `pay_${unique}`,
      order_id: `order_${unique}`,
      user_id: `user_${unique}`,
      amount: 1990,
      currency: "RUB"
    },
    max_attempts: 3,
    idempotency_key: `loadtest:immediate:${unique}`
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
