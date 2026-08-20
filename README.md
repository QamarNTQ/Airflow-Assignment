# DoughFlow Pizza Co. Pipeline

## 1. Task Flow & Design
* **Flow:** `take_order` -> `check_stock` -> Branch (`bake_pizza` branch OR `cancel_order`) -> `log_final_status`.
* **Design:** Mimics a real kitchen. Branching early prevents out-of-stock orders from wasting time and resources in the baking/delivery pipeline.

## 2. XCom Data
* **Data:** A dictionary (`order_data`) containing `order_id`, `pizza_type`, and `is_premium`.
* **Reason:** Shares order context across tasks. It allows downstream steps to check ingredient availability, adjust oven times dynamically, and log accurate order statuses.

## 3. Skip Conditions & Trigger Rules
* **Skip Condition:** If the selected pizza contains `"paneer"`, `check_stock` routes to `cancel_order`, automatically skipping the baking, QA, packing, and delivery steps.
* **Trigger Rule:** `log_final_status` uses `NONE_FAILED_MIN_ONE_SUCCESS`. This ensures the final status is logged successfully whether the order was delivered or cancelled.

## 4. Schedule Selection
* **Schedule:** Manual / Ad-hoc (`schedule=None`, `catchup=False`).
* **Reason:** Kitchen orders are reactive and unpredictable. The DAG must trigger instantly via API webhooks when a customer places an order, rather than running on a fixed time interval.
