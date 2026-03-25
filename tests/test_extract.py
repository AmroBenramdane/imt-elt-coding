"""
TP3 — Unit tests for src/extract.py
=====================================

These tests verify that extraction functions correctly read from S3
and load into Bronze, without needing real AWS or database connections.

We mock:
  - _get_s3_client → so we don't need real AWS credentials
  - _load_to_bronze → so we don't need a real database
  - _read_csv_from_s3 / _read_jsonl_from_s3 → to inject fake data
"""

import pandas as pd
import pytest
from unittest.mock import patch, MagicMock

from src.extract import (
    extract_products,
    extract_users,
    extract_orders,
    extract_order_line_items,
    extract_reviews,
    extract_clickstream
)


class TestExtractProducts:
    """Tests for extract_products()."""

    @patch("src.extract._load_to_bronze")
    @patch("src.extract._read_csv_from_s3")
    def test_extracts_and_loads(self, mock_read_csv, mock_load, sample_products):
        # TODO: Test that extract_products reads from S3 and loads to Bronze
        # Steps:
        #   1. mock_read_csv.return_value = sample_products
        #   2. result = extract_products()
        #   3. Assert result has the expected number of rows
        #   4. Assert mock_load was called (mock_load.assert_called_once())
        mock_read_csv.return_value = sample_products
        result = extract_products()
        
        assert len(result) == len(mock_read_csv.return_value)
        mock_load.assert_called_once()

    @patch("src.extract._load_to_bronze")
    @patch("src.extract._read_csv_from_s3")
    def test_returns_dataframe(self, mock_read_csv, mock_load, sample_products):
        # TODO: Test that the function returns a pandas DataFrame
        # Hint: isinstance(result, pd.DataFrame)
        mock_read_csv.return_value = sample_products
        result = extract_products()
        
        assert isinstance(result, pd.DataFrame)


class TestExtractUsers:
    """Tests for extract_users()."""

    @patch("src.extract._load_to_bronze")
    @patch("src.extract._read_csv_from_s3")
    def test_extracts_and_loads(self, mock_read_csv, mock_load, sample_users):
        # TODO: Same pattern as TestExtractProducts
        mock_read_csv.return_value = sample_users
        result = extract_users()
        
        assert len(result) == len(mock_read_csv.return_value)
        mock_load.assert_called_once()
    
    @patch("src.extract._load_to_bronze")
    @patch("src.extract._read_csv_from_s3")
    def test_returns_dataframe(self, mock_read_csv, mock_load, sample_users):
        # TODO: Test that the function returns a pandas DataFrame
        # Hint: isinstance(result, pd.DataFrame)
        mock_read_csv.return_value = sample_users
        result = extract_users()
        
        assert isinstance(result, pd.DataFrame)


class TestExtractOrders:
    """Tests for extract_orders()."""

    @patch("src.extract._load_to_bronze")
    @patch("src.extract._read_csv_from_s3")
    def test_extracts_and_loads(self, mock_read_csv, mock_load, sample_orders):
        # TODO: Same pattern as TestExtractProducts
        mock_read_csv.return_value = sample_orders
        result = extract_orders()
        
        assert len(result) == len(mock_read_csv.return_value)
        mock_load.assert_called_once()
    
    @patch("src.extract._load_to_bronze")
    @patch("src.extract._read_csv_from_s3")
    def test_returns_dataframe(self, mock_read_csv, mock_load, sample_orders):
        # TODO: Test that the function returns a pandas DataFrame
        # Hint: isinstance(result, pd.DataFrame)
        mock_read_csv.return_value = sample_orders
        result = extract_orders()
        
        assert isinstance(result, pd.DataFrame)

class TestExtractOrderLineItems:
    """Tests for extract_orders()."""

    @patch("src.extract._load_to_bronze")
    @patch("src.extract._read_csv_from_s3")
    def test_extracts_and_loads(self, mock_read_csv, mock_load, sample_order_line_items):
        # TODO: Same pattern as TestExtractProducts
        mock_read_csv.return_value = sample_order_line_items
        result = extract_order_line_items()
        
        assert len(result) == len(mock_read_csv.return_value)
        mock_load.assert_called_once()
    
    @patch("src.extract._load_to_bronze")
    @patch("src.extract._read_csv_from_s3")
    def test_returns_dataframe(self, mock_read_csv, mock_load, sample_order_line_items):
        # TODO: Test that the function returns a pandas DataFrame
        # Hint: isinstance(result, pd.DataFrame)
        mock_read_csv.return_value = sample_order_line_items
        result = extract_order_line_items()
        
        assert isinstance(result, pd.DataFrame)

class TestExtractReviews:
    """Tests for extract_reviews()."""

    @patch("src.extract._load_to_bronze")
    @patch("src.extract._read_jsonl_from_s3")
    def test_extracts_and_loads(self, mock_read_jsonl, mock_load, sample_reviews):
        # TODO: Same pattern as TestExtractProducts
        mock_read_jsonl.return_value = sample_reviews
        result = extract_reviews()
        
        assert len(result) == len(mock_read_jsonl.return_value)
        mock_load.assert_called_once()
    
    @patch("src.extract._load_to_bronze")
    @patch("src.extract._read_jsonl_from_s3")
    def test_returns_dataframe(self, mock_read_jsonl, mock_load, sample_reviews):
        # TODO: Test that the function returns a pandas DataFrame
        # Hint: isinstance(result, pd.DataFrame)
        mock_read_jsonl.return_value = sample_reviews
        result = extract_reviews()
        
        assert isinstance(result, pd.DataFrame)

class TestExtractClickstream:
    """Tests for extract_clickstream()."""

    @patch("src.extract._load_to_bronze")
    @patch("src.extract._read_partitioned_parquet_from_s3")
    def test_extracts_and_loads(self, mock_read_parquet, mock_load, sample_clickstream):
        # TODO: Same pattern as TestExtractProducts
        mock_read_parquet.return_value = sample_clickstream
        result = extract_clickstream()
        
        assert len(result) == len(mock_read_parquet.return_value)
        mock_load.assert_called_once()
    
    @patch("src.extract._load_to_bronze")
    @patch("src.extract._read_partitioned_parquet_from_s3")
    def test_returns_dataframe(self, mock_read_parquet, mock_load, sample_clickstream):
        # TODO: Test that the function returns a pandas DataFrame
        # Hint: isinstance(result, pd.DataFrame)
        mock_read_parquet.return_value = sample_clickstream
        result = extract_clickstream()
        
        assert isinstance(result, pd.DataFrame)
    
    
