"""
TP3 — Shared pytest fixtures
=============================
Fixtures are fake DataFrames that mimic Bronze data.
They are automatically injected into tests by pytest when a test parameter
has the same name as a fixture function.

Example:
    # This fixture is defined here:
    @pytest.fixture
    def sample_products(): ...

    # Any test with "sample_products" as a parameter receives it automatically:
    def test_something(self, sample_products):
        # sample_products is the DataFrame returned by the fixture above
"""

import pytest
import pandas as pd


@pytest.fixture
def sample_products():
    """Fake products DataFrame mimicking Bronze data."""
    return pd.DataFrame({
        "product_id": [1, 2, 3],
        "display_name": ["Nike Air Max", "Adidas Ultraboost", "Jordan 1"],
        "brand": ["Nike", "Adidas", "Jordan"],
        "category": ["sneakers", "sneakers", "sneakers"],
        "price_usd": [149.99, 179.99, -10.00],  # one invalid price for testing
        "tags": ["running|casual", "running|boost", "retro|hype"],
        "is_active": [1, 1, 0],
        "is_hype_product": [0, 0, 1],
        "_internal_cost_usd": [50.0, 60.0, 70.0],
        "_supplier_id": ["SUP001", "SUP002", "SUP003"],
    })


@pytest.fixture
def sample_users():
    """Fake users DataFrame mimicking Bronze data."""
    # TODO: Create a DataFrame with these columns:
    #   - user_id: [1, 2]
    #   - email: include one with spaces and uppercase to test normalization
    #            e.g. [" Alice@Example.COM ", "bob@test.com"]
    #   - first_name, last_name: any values
    #   - loyalty_tier: include one None to test fillna
    #   - _hashed_password, _last_ip, _device_fingerprint: internal columns (should be removed)
    #
    # Hint: follow the same pattern as sample_products above

    return pd.DataFrame({
        "user_id": [1, 2],
        "email": [" Alice@Example.COM ", "bob@test.com"],
        "first_name": ["Alice", "Bob"],
        "last_name": ["Martin", "Smith"],
        "loyalty_tier": ["gold", None],
        "_hashed_password": ["abc123", "def456"],
        "_last_ip": ["1.2.3.4", "5.6.7.8"],
        "_device_fingerprint": ["fp1", "fp2"],
    })


@pytest.fixture
def sample_orders():
    """Fake orders DataFrame mimicking Bronze data."""
    # TODO: Create a DataFrame with these columns:
    #   - order_id: [1, 2, 3]
    #   - user_id: [1, 2, 1]
    #   - order_date: string dates like ["2026-02-10", "2026-02-11", "2026-02-12"]
    #   - status: include one invalid status like "invalid_status" to test filtering
    #   - total_usd: any positive values
    #   - coupon_code: include one None to test fillna
    #   - _stripe_charge_id, _fraud_score: internal columns (should be removed)
    #
    # Hint: follow the same pattern as sample_products above

    return pd.DataFrame({
        "order_id": [1, 2, 3],
        "user_id": [1, 2, 1],
        "order_date": ["2026-02-10", "2026-02-11", "2026-02-12"],
        "status": ["delivered", "shipped", "invalid_status"],
        "total_usd": [149.99, 179.99, 50.0],
        "coupon_code": ["SAVE10", None, None],
        "_stripe_charge_id": ["ch_1", "ch_2", "ch_3"],
        "_fraud_score": [0.1, 0.2, 0.9],
    })

@pytest.fixture
def sample_order_line_items():
    """Fake order line items DataFrame mimicking Bronze data."""
    
    return pd.DataFrame({
        "line_item_id": [1, 2, 3],
        "order_id": [1, 1, 2],
        "product_id": [101, 102, 103],
        "selected_size_colorway": ["M / Red", "L / Blue", "S / Black"],
        "quantity": [1, 2, 0],
        "unit_price_usd": [49.99, 59.99, 79.99],
        "line_total_usd": [49.99, 119.98, 79.99],
        "_warehouse_id": ["WH1", "WH1", "WH2"],
        "_internal_batch_code": ["BATCH1", "BATCH2", "BATCH3"],
        "_pick_slot": ["A1", "B2", "C3"],
    })

@pytest.fixture
def sample_reviews():
    """Fake reviews DataFrame mimicking Bronze JSONL data."""
    
    return pd.DataFrame({
        "review_id": [1, 2, 3],
        "product_id": [101, 102, 101],
        "user_id": [1, 2, 3],
        "rating": [5, 3, 1],
        "title": ["Excellent", "Correct", "Très déçu"],
        "body": ["Produit parfait", None, "Mauvaise qualité"],
        "verified_purchase": [True, False, True],
        "moderation_status": ["approved", "pending", "rejected"],
        "helpful_votes": [10, 2, 0],
        "_sentiment_raw": [0.95, 0.2, -0.8],
        "_toxicity_score": [0.01, 0.05, 0.7],
    })

@pytest.fixture
def sample_clickstream():
    """Fake clickstream DataFrame mimicking partitioned Parquet Bronze data."""
    
    return pd.DataFrame({
        "event_id": ["evt_1", "evt_2", "evt_3", "evt_4"],
        "event_type": ["pageview", "pageview", "pageview", "pageview"],
        "timestamp": [
            "2026-02-10 10:00:00",
            "2026-02-10 10:05:00",
            "2026-02-11 11:00:00",
            "2026-02-11 11:10:00",
        ],
        "user_id": [1, None, 2, None],
        "session_id": ["sess_1", "sess_1", "sess_2", "sess_3"],
        "page_url": [
            "https://kickz.com/home",
            "https://kickz.com/product/101",
            "https://kickz.com/cart",
            "https://kickz.com/home",
        ],
        "page_path": ["/home", "/product/101", "/cart", "/home"],
        "page_type": ["home", "product", "cart", "home"],
        "referrer_url": [
            "https://google.com",
            "https://kickz.com/home",
            None,
            "https://instagram.com",
        ],
        "referrer_source": ["google", "internal", None, "social"],
        "user_agent_raw": ["Mozilla/5.0", "Mozilla/5.0", "Bot/1.0", "Mozilla/5.0"],
        "ip_address": ["192.168.1.1", "192.168.1.2", "10.0.0.1", "192.168.1.3"],
        "viewport_width": [1920, 1366, 800, 1920],
        "viewport_height": [1080, 768, 600, 1080],
        "is_bot": [False, False, True, False],
        "_ga_client_id": ["ga1", "ga2", "ga3", "ga4"],
        "_dom_interactive_ms": [120.5, 200.1, 300.0, 150.2],
        "_dom_complete_ms": [300.5, 400.1, 500.0, 350.2],
        "_ttfb_ms": [50.2, 70.1, 90.0, 60.3],
        "_internal_1": [1, 2, 3, 4],
        "_internal_2": [1, 2, 3, 4],
        "_internal_3": [1, 2, 3, 4],
        "_internal_4": [1, 2, 3, 4],
    })