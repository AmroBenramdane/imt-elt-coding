import pandas as pd
import pytest
from unittest.mock import patch, MagicMock

from src.gold import (
    create_product_performance
)


class TestCreateProductPerformance:
    """Tests for create_product_performance()."""

    @patch("src.gold._create_gold_table")
    @patch("src.gold.pd.read_sql")
    def test_orders_of_total_revenue(self, mock_sql, mock_create, sample_product_performance):

        mock_sql.return_value = sample_product_performance
        create_product_performance()

        df_result = mock_create.call_args[0][0]
        revenues = df_result['total_revenue'].tolist()
        for i in range(len(revenues) - 1):
            assert revenues[i] >= revenues[i+1], "Products are not sorted by total_revenue DESC"

   
