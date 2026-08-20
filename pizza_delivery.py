from datetime import datetime, timedelta
import logging
import random
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.trigger_rule import TriggerRule


logger = logging.getLogger("airflow.task")

PIZZERIA_NAME = "DoughFlow Pizza Co."
OUT_OF_STOCK_TOPPING = "paneer"


default_args = {
    'owner': 'data_engineer',7
    'depends_on_past': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}


with DAG(
    'pizza_processing_dag',
    default_args=default_args,
    description='Automated kitchen pipeline for processing and delivering pizza',
    start_date = datetime(2026, 8, 20),
    catchup=False,
    tags=['kitchen_automation'],
) as dag:


    def take_order(task_instance):
        logger.info(f"Starting order processig for {PIZZERIA_NAME}")

        pizzas = ["Farmhouse", "Margherita", "Cheesy Paneer", "BBQ Chicken"]
        selected_pizza = random.choice(pizzas)
        order_id = f"DFP-{random.randint(1000, 9999)}"

        order_data = {
            "order_id": order_id,
            "pizza_type": selected_pizza,
            "is_premium": selected_pizza in ["Cheesy Paneer", "BBQ Chicken"]
        }

        task_instance.xcom_push(key="order_data", value=order_data)
        logger.info(f"Order selected successfully: {order_id} | Pizza: {selected_pizza}")



    def check_stock(task_instance):
        payload = task_instance.xcom_pull(task_ids='take_order', key='order_data')
        pizza_name = payload["pizza_type"].lower()

        logger.info(f"Checking ingredients stock for Order {payload['order_id']}.")

        if OUT_OF_STOCK_TOPPING in pizza_name:
            logger.warning(
                f"Ingredient: {OUT_OF_STOCK_TOPPING} is out of stock "
                f"Sending Order {payload['order_id']} to the cancellation step."
            )
            return 'cancel_order'

        logger.info("Everything is in stock. Sending the order to the kitchen to bake.")
        return 'bake_pizza'



    def bake_pizza(task_instance):
        payload = task_instance.xcom_pull(task_ids='take_order', key='order_data')
        logger.info(f"Making a fresh {payload['pizza_type']} pizza now")

        bake_time = 450 if payload["is_premium"] else 300
        logger.info(f"Putting it in the oven for {bake_time} seconds.")
        logger.info(f"Order {payload['order_id']} successfully baked")



    def check_quality(task_instance):
        payload = task_instance.xcom_pull(task_ids='take_order', key='order_data')
        logger.info(f"Running Quality check for Order {payload['order_id']}.")

        score = random.randint(8, 10)
        logger.info(f"Quality Score: {score}/10. Passed inspection")



    def start_delivery(task_instance):
        payload = task_instance.xcom_pull(task_ids='take_order', key='order_data')
        logger.info(f"Finding the best delivery route for Order {payload['order_id']}.")
        logger.info(f"Driver has picked up the order and departed. Workflow finished successfully!")



    def final_status(task_instance):
        payload = task_instance.xcom_pull(task_ids='take_order', key='order_data')
        order_id = payload['order_id']

        delivery_info = task_instance.xcom_pull(task_ids='start_delivery')
        cancel_info = task_instance.xcom_pull(task_ids='cancel_order')

        if delivery_info:
            message = f"Order '{order_id}' Successfully delivered"
        else:
            message = f"Order '{order_id}' was cancelled and refunded"

        logger.info(message)
        return message


    take_order = PythonOperator(
        task_id='take_order',
        python_callable=take_order,
    )

    check_stock = BranchPythonOperator(
        task_id='check_stock',
        python_callable=check_stock,
    )

    bake_pizza = PythonOperator(
        task_id='bake_pizza',
        python_callable=bake_pizza,
    )

    cancel_order = BashOperator(
        task_id='cancel_order',
        bash_command='echo "Order cancelled: missing ingredients. Processing automatic customer refund"'
    )

    check_quality = PythonOperator(
        task_id='check_quality',
        python_callable=check_quality,
        trigger_rule=TriggerRule.ONE_SUCCESS,
    )

    pack_box = BashOperator(
        task_id='pack_box',
        bash_command='''
          echo "Folding the pizza box."
          echo "Putting the pizza in the insulated thermal bag."
          echo "Sealing the box with safety tape."
        ''',
    )

    start_delivery = PythonOperator(
        task_id='start_delivery',
        python_callable=start_delivery,
    )

    final_status = PythonOperator(
        task_id='log_final_status',
        python_callable=final_status,
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )


    take_order >> check_stock
    check_stock >> [bake_pizza, cancel_order]
    bake_pizza >> check_quality >> pack_box >> start_delivery
    cancel_order >> final_status