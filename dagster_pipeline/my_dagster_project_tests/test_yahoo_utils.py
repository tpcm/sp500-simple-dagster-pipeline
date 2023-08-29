"""_summary_

Returns:
    _type_: _description_
"""
from unittest import mock
import pytest
from pytest_mock import mocker
from requests.exceptions import HTTPError

from my_dagster_project.assets.yahoo_utils import get_ticker_info


@pytest.fixture
def mock_yf_ticker(mocker):
    return mocker.patch(
        'my_dagster_project.assets.yahoo_utils.yf.Ticker'
    )

@pytest.fixture
def mock_pandas(mocker):
    return mocker.patch(
        'my_dagster_project.assets.yahoo_utils.pd'
    )

@pytest.mark.parametrize(
    "ticker,info_wanted,expected_return",
    [
        ('AAPL',['marketCap'],{'marketCap':100000}),
    ]
)
def test_get_ticker_info_single_field(
    ticker,
    info_wanted,
    expected_return,
    mock_yf_ticker,
):
    """_summary_

    Args:
        ticker (_type_): _description_
        info_wanted (_type_): _description_
        expected_return (_type_): _description_
        mock_yf_ticker (_type_): _description_
    """    
    # Mock return value of yfinance.Ticker.info
    mock_yf_ticker.return_value.info = expected_return

    # Call fn being tested
    result = get_ticker_info(ticker=ticker, info_wanted=info_wanted)

    assert result == expected_return



# @pytest.mark.parametrize(
#     "ticker,info_wanted,expected_return",
#     [
#         ('AAPL',['marketCap', 'trailingPE'],[100000, 25.891342]),
#     ]
# )