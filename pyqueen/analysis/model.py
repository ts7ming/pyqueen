import numpy as np
import pandas as pd


class Model:
    @staticmethod
    def arima(data, forecast_step: int, p: int = None, d: int = None, q: int = None):
        if isinstance(data, pd.Series) is False:
            data = pd.Series(data)
        if p is not None and d is not None and q is not None:
            from statsmodels.tsa.arima.model import ARIMA
            model = ARIMA(data, order=(p, d, q))
            model_fit = model.fit()
        else:
            import pmdarima as pm
            model = pm.auto_arima(data, seasonal=False, trace=False)
            model_fit = model.fit(data)
        forecast = model_fit.predict(n_periods=forecast_step).tolist()[-1 * forecast_step:]
        return forecast

    @staticmethod
    def kmeans(data, n_clusters=None):
        if isinstance(data, np.ndarray) is False:
            data = np.array(data)
        from sklearn.cluster import KMeans
        kmeans = KMeans(n_clusters=3)
        kmeans.fit(data)
        return kmeans
