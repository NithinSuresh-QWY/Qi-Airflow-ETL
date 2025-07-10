from etl_main.load_config import load_config
from etl_main.sql_renderer import render_query
from utils.db_utils import get_sqlalchemy_conn
from utils.extract import get_last_created_on
from sqlalchemy import text
import logging

logger = logging.getLogger(__name__)


def run_post_calculations(so_date_before_pull, fetch_until_date=None):
    config = load_config()
    target_conn_id = config["databases"]["target"]["conn_id"]
    engine = get_sqlalchemy_conn(target_conn_id)

    try:
        with engine.begin() as conn:  # transactional context
            last_created_on_so = get_last_created_on(conn, 'sale_orders', config)
            logger.info(f"so_date_before_pull = {so_date_before_pull}")
            queries = {
                "Order Cost": render_query("actl_and_est_order_cost_cal.sql", {
                    "last_created_on": last_created_on_so
                }),
                "Sale orders Update": render_query("so_actl_est_update.sql", {
                    "so_date_before_pull": so_date_before_pull
                }),
                "Rider Avg Cost": render_query("rider_avg_cost_cal.sql", {})
            }

            for desc, query in queries.items():
                if query.strip():
                    logger.debug(f"Executing query for {desc}: {query}")
                    logger.info(f"▶️ Executing {desc}")
                    result = conn.execute(text(query))
                    logger.info(f"✅ {desc} affected {result.rowcount} rows")
    except Exception as e:
        logger.error(f"❌ Error during post-calculations: {str(e)}")
        raise
    finally:
        engine.dispose()
