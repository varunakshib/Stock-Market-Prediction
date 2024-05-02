# from jugaad_data.nse import stock_df
from pyspark.sql.functions import mean, stddev, col, lag, avg, when, sum, abs
from pyspark.sql import Window
from pyspark.sql import SparkSession
from pyspark.ml import PipelineModel
from pyspark.ml.feature import VectorAssembler
from datetime import datetime, timedelta
import os
import sys
import yfinance as yf
import yahoo_fin.stock_info as si

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable


print("creating session")
spark = SparkSession.builder \
    .appName("StockMarketPrediction") \
    .getOrCreate()


def calculate_features_for_last_record(data):
    # Calculate moving averages
    windowSpec20 = Window.orderBy('Date').rowsBetween(-19, 0)
    windowSpec50 = Window.orderBy('Date').rowsBetween(-49, 0)
    windowSpec200 = Window.orderBy('Date').rowsBetween(-199, 0)

    data = data.withColumn('20d_moving_avg', mean(col('Close')).over(windowSpec20))
    data = data.withColumn('50d_moving_avg', mean(col('Close')).over(windowSpec50))
    data = data.withColumn('200d_moving_avg', mean(col('Close')).over(windowSpec200))

    # Calculate daily returns
    data = data.withColumn('daily_return', (col('Close') - lag('Close', 1).over(Window.orderBy('Date'))) / lag('Close', 1).over(Window.orderBy('Date')))

    # Calculate volatility
    windowSpecVolatility = Window.orderBy('Date').rowsBetween(-19, 0)
    data = data.withColumn('volatility', stddev(col('daily_return')).over(windowSpecVolatility))

    # Calculate change, gain, loss
    data = data.withColumn('change', col('Close') - lag('Close', 1).over(Window.orderBy('Date')))
    data = data.withColumn('gain', when(col('change') > 0, col('change')).otherwise(0))
    data = data.withColumn('loss', when(col('change') < 0, -col('change')).otherwise(0))

     # Additional features
    data = data.withColumn('ema_10', avg(col('Close')).over(Window.orderBy('Date').rowsBetween(-9, 0)))
    data = data.withColumn('ema_20', avg(col('Close')).over(Window.orderBy('Date').rowsBetween(-19, 0)))
    data = data.withColumn('macd', col('ema_10') - col('ema_20'))
    data = data.withColumn('rsi', 100 - (100 / (1 + (avg(when(col('daily_return') > 0, col('daily_return'))).over(Window.orderBy('Date').rowsBetween(-13, 0)) /
                                                avg(when(col('daily_return') < 0, -col('daily_return'))).over(Window.orderBy('Date').rowsBetween(-13, 0))))))
  # Bollinger Bands
    data = data.withColumn('stddev_20', stddev(col('Close')).over(windowSpec20))
    data = data.withColumn('bollinger_upper', col('20d_moving_avg') + 2 * col('stddev_20'))
    data = data.withColumn('bollinger_lower', col('20d_moving_avg') - 2 * col('stddev_20'))

    # Average True Range (ATR)
    data = data.withColumn('tr', when(col('High') - col('Low') > abs(col('High') - lag('Close', 1).over(Window.orderBy('Date'))), col('High') - col('Low')).otherwise(when(abs(col('High') - lag('Close', 1).over(Window.orderBy('Date'))) > abs(col('Low') - lag('Close', 1).over(Window.orderBy('Date'))), abs(col('High') - lag('Close', 1).over(Window.orderBy('Date')))).otherwise(abs(col('Low') - lag('Close', 1).over(Window.orderBy('Date'))))))
    data = data.withColumn('atr', avg(col('tr')).over(windowSpec20))

    # Moving Average Convergence Divergence Signal (MACD Signal)
    data = data.withColumn('macd_signal', avg(col('macd')).over(Window.orderBy('Date').rowsBetween(-8, 0)))



    # Select the last record and dropna
    last_record = data.orderBy(col("Date").desc()).limit(1).dropna()

    return last_record

def preprocess_input_data(input_data):
    assembler = VectorAssembler(
        inputCols=['Open', 'High', 'Low', 'Close', 'Volume',
                   '20d_moving_avg', '50d_moving_avg', '200d_moving_avg',
                   'daily_return', 'volatility', 'change', 'gain', 'loss', 'ema_10', 'ema_20', 'macd', 'rsi', 'tr', 'atr', 'macd_signal'],
        outputCol='features')

    preprocessed_data = assembler.transform(input_data)

    return preprocessed_data

def predict(model, input_data):
    preprocessed_data = preprocess_input_data(input_data)

    # Make predictions
    predictions = model.transform(preprocessed_data)

    # Return predictions
    return predictions

def get_model_name(company_name):
    model_name = ""
    if company_name == "Tata Motors":
        model_name = "Tatamotors"
    if company_name == "HDFC Bank":
        model_name = "Hdfc"
    if company_name == "Cipla":
        model_name = "Cipla"
    if company_name == "Wipro":
        model_name = "Wipro"
    return model_name

def get_company_code(company_name):
    company_code = ""
    if company_name == "Tata Motors":
        company_code = "TATAMOTORS.NS"
    if company_name == "HDFC Bank":
        company_code = "HDFCBANK.NS"
    if company_name == "Cipla":
        company_code = "CIPLA.NS"
    if company_name == "Wipro":
        company_code = "WIPRO.NS"
    return company_code
def predict_price(selected_date, company_name):
    end_date = datetime.strptime(selected_date, "%Y-%m-%d").date() - timedelta(days=0)
    start_date = end_date - timedelta(days=200)  # Last 200 days from yesterday
    print(end_date)
    print(start_date)
    end_date_str = end_date.strftime('%Y-%m-%d')
    start_date_str = start_date.strftime('%Y-%m-%d')
    print(end_date_str)
    print(start_date_str)
    company_code = get_company_code(company_name)
    # current_stock_data = yf.download(company_code, start=start_date_str, end=end_date_str)
    # current_stock_data = stock_df(symbol='TATAMOTORS',
    #                               from_date=start_date,
    #                               to_date=end_date,
    #                               series="EQ")
    #
    current_stock_data = si.get_data(company_code, start_date=start_date_str, end_date=end_date_str)
    current_stock_data.reset_index(inplace=True)
    current_stock_data.columns.values[0] = 'Date'
    current_stock_data.rename(columns=lambda x: x.title(), inplace=True)
    print("created the data frame")
    current_stock_data = spark.createDataFrame(current_stock_data.reset_index())
    print("setted")

    last_record_features = calculate_features_for_last_record(current_stock_data)
    last_record_features.show()
    model_name = get_model_name(company_name)
    model = PipelineModel.load(f"/content/Stock-Market-Prediction/models/{model_name}")
    predictions = predict(model, last_record_features)
    prediction_price_value = predictions.select("prediction").first()[0]
    print(prediction_price_value)
    data={}
    data["date"] = predictions.select("Date").first()[0]
    data["open"] = predictions.select("Open").first()[0]
    data["high"] = predictions.select("High").first()[0]
    data["low"] = predictions.select("Low").first()[0]
    data["close"] = predictions.select("Close").first()[0]
    data["next_close"] = predictions.select("prediction").first()[0]
    return data