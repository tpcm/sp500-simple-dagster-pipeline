"""
utils.py - Utility Functions Module

This module contains various utility functions that can be used across different parts
of the application.

Notes:
------
- Additional notes or important information about the module and its functions can be mentioned here.
- For more details, refer to the individual function docstrings.
"""


import datetime as dt
from typing import List, Optional, Any, Dict

import pandas as pd
import yfinance as yf


def get_ticker_info(ticker: str, info_wanted: List[str]) -> Dict:
    """
    Get market cap and current price information for a given stock ticker.

    Parameters:
    ------------
    ticker : str
        The stock ticker symbol for which to retrieve price information.

    Returns:
    ---------
    pd.DataFrame:
        A DataFrame containing market cap and current price information.

    Notes:
    ------
    The function uses the yfinance library to fetch stock information.
    If any errors occur during the retrieval, the function returns None.
    """
    try:
        tick = yf.Ticker(ticker)
        tick_info = tick.info
    except Exception:
        return None

    info_retrieved = {}
    for i in info_wanted:
        try:
            info_retrieved[i] = tick_info[i]
        except KeyError:
            info_retrieved[i] = None

    return info_retrieved


def calculate_period_return(ticker: str, period: str) -> float:
    """
    Calculate the percentage return of a stock over a specified period using yfinance library.

    Parameters:
    -----------
    ticker : str
        The stock ticker symbol (e.g., AAPL for Apple Inc.) for which to calculate the period return.

    period : str
        The time period for which to calculate the return. It should be a string representing the duration
        in Yahoo Finance format (e.g., '1d' for one day, '1mo' for one month, '3mo' for three months, '1y' for
        one year, '5y' for five years).

    Returns:
    --------
    float:
        The percentage return of the stock over the specified period. A positive value indicates a positive
        return (stock has gained value), and a negative value indicates a negative return (stock has declined
        in value).

    Raises:
    -------
    ValueError:
        If the provided `ticker` is invalid or not found in the financial data.

    Notes:
    ------
    - The function uses yfinance library to fetch historical stock price data for the specified period.
    - The percentage return is calculated as (Ending Price - Starting Price) / Starting Price * 100.
    - The function assumes that the stock data contains daily closing prices for the specified period.
    - Please make sure you have an internet connection to fetch the latest stock data from yfinance.

    Example:
    --------
    >>> calculate_period_return('AAPL', '1y')
    32.45
    """
    stock_data = yf.Ticker(ticker).history(period=period)
    first_close = stock_data["Close"][0]
    last_close = stock_data["Close"][-1]

    return (last_close - first_close) / first_close * 100


def calculate_price_to_earnings(
    ticker: Optional[str], ticker_info: Optional[pd.DataFrame] = None
):
    """
    REDUNDANT AS CAN JUST PULL IN TRAILING PE FROM CLASS ATTRIBUTES 'trailingPE'
    Calculate the Price-to-Earnings (P/E) ratio for a given stock ticker.

    Parameters:
    -----------
        ticker (str): Ticker symbol of the stock.

    Returns:
    --------
        float: The calculated P/E ratio.
    """
    if not ticker_info:
        tick = yf.Ticker(ticker)
        tick_info = tick.info

    stock_data = tick.history(period="12mo")
    eps = tick_info["trailingEps"]
    current_price = stock_data["Close"][-1]

    return current_price / eps


def calculate_price_to_cash_flow(ticker: Optional[str]):
    """
    Calculate the price to operating cash flow ratio.

    This function calculates the ratio of the thirty-day mean share price to the operating cash flow for a given stock ticker.

    Parameters:
    ticker (str): The stock ticker symbol.
    ticker_info (pd.DataFrame): Optional. DataFrame containing stock information. If not provided, the function will fetch the data using yfinance.

    Returns:
    float: The price to operating cash flow ratio.
    """
    try:
        tick_info = ticker.info

        stock_data = ticker.history(period="12mo")
        thirty_day_mean_share_price = stock_data["Close"][-30:].mean()
        operating_cash_flow = tick_info["operatingCashflow"]
    except:
        return None

    return thirty_day_mean_share_price / operating_cash_flow


def scrape_stock_measures(
    stocks: pd.DataFrame,
):
    my_cols = [
        "ticker",
        "current_price",
        "trailing_pe",
        "trailing_earnings_per_share",
        "free_cash_flow",
        "cash_per_share",
        "price_to_cash_flow",
        "return_on_equity",
        'date',
    ]
    final_df = pd.DataFrame(columns=my_cols)

    bad_tickers = []
    for _, row in stocks.iterrows():
        ticker = row.values[0]
        try:
            tick = yf.Ticker(ticker)
            tick_info = tick.info
            to_append = pd.Series(
                [
                    ticker,
                    tick_info["currentPrice"],
                    tick_info["trailingPE"],
                    tick_info["trailingEps"],
                    tick_info["freeCashflow"],
                    tick_info["totalCashPerShare"],
                    calculate_price_to_cash_flow(ticker=tick),
                    tick_info["returnOnEquity"],
                    dt.datetime.now().date(),
                ],
                index=my_cols,
            )

            final_df = pd.concat([final_df, to_append.to_frame().T], ignore_index=True)
        except:
            bad_tickers.append(ticker)

    return {"stock_measures": final_df, "bad_tickers": bad_tickers}
