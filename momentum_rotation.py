# momentum_rotation.py
import backtrader as bt
import yfinance as yf
import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime, timedelta
import warnings
warnings.filterwarnings("ignore")

# ==================== 1. 自定义策略 ====================
class MomentumRotationStrategy(bt.Strategy):
    params = (
        ('lookback', 252),        # 动量计算周期：1年（约252交易日）
        ('top_n', 5),             # 每月选前5只
        ('rs_threshold', 70),     # RS Rating > 70 才入池
        ('rebalance_day', 1),     # 每月第1个交易日再平衡
        ('stake', 100),           # 每只股买入100股（可改为等权）
        ('printlog', True),
    )

    def __init__(self):
        self.returns = {}  # 存储每只股票的动量
        self.order = None
        self.rebalance_month = None

    def log(self, txt, dt=None):
        if self.p.printlog:
            dt = dt or self.datas[0].datetime.date(0)
            print(f'{dt.isoformat()}, {txt}')

    def notify_order(self, order):
        if order.status in [order.Completed]:
            if order.isbuy():
                self.log(f'BUY EXECUTED, {order.data._name}, Price: {order.executed.price:.2f}, Size: {order.executed.size}')
            elif order.issell():
                self.log(f'SELL EXECUTED, {order.data._name}, Price: {order.executed.price:.2f}, Size: {order.executed.size}')

    def prenext(self):
        pass  # 等待所有数据对齐

    def next(self):
        # 每月第1个交易日再平衡
        current_date = self.datas[0].datetime.date(0)
        if self.rebalance_month == current_date.month:
            return
        if current_date.day != self.p.rebalance_day:
            return

        self.rebalance_month = current_date.month
        self.log(f'--- REBALANCING ON {current_date} ---')

        # Step 1: 计算每只股票的动量（过去lookback日回报率）
        momentum_dict = {}
        benchmark = self.datas[0]  # 假设第0个是SPY作为基准
        spy_return = (benchmark.close[0] / benchmark.close[-self.p.lookback]) - 1 if len(benchmark) > self.p.lookback else 0

        for d in self.datas[1:]:  # 跳过SPY
            if len(d) < self.p.lookback:
                continue
            ret = (d.close[0] / d.close[-self.p.lookback]) - 1
            # 计算 RS Rating
            rs_rating = (ret / spy_return * 100) if spy_return != 0 else 0
            if rs_rating > self.p.rs_threshold:
                momentum_dict[d._name] = ret

        # Step 2: 按动量排序，取前 top_n
        ranked = sorted(momentum_dict.items(), key=lambda x: x[1], reverse=True)[:self.p.top_n]
        targets = [name for name, _ in ranked]

        self.log(f"Top {self.p.top_n} Momentum Stocks: {targets}")

        # Step 3: 卖出不在目标中的持仓
        for d in self.datas[1:]:
            if d._name not in targets and self.getposition(d).size > 0:
                self.order_target_percent(d, target=0)

        # Step 4: 等权买入目标股票
        weight = 1.0 / len(targets) if targets else 0
        for name in targets:
            for d in self.datas[1:]:
                if d._name == name:
                    self.order_target_percent(d, target=weight)
                    break

    def stop(self):
        self.log(f'Final Portfolio Value: {self.broker.getvalue():.2f}')


# ==================== 2. 数据准备 ====================
def get_data(tickers, start, end):
    data = {}
    for t in tickers:
        df = yf.download(t, start=start, end=end, progress=False)
        if df.empty:
            print(f"Warning: No data for {t}")
            continue
        df = df[['Open', 'High', 'Low', 'Close', 'Volume']]
        df.dropna(inplace=True)
        data[t] = df
    return data


# ==================== 3. 主函数 ====================
if __name__ == '__main__':
    # 股票池（可替换为你关注的成长股/科技股）
    universe = [
        'SPY',    # 基准
        'AAPL', 'MSFT', 'GOOGL', 'AMZN', 'NVDA',
        'TSLA', 'META', 'NFLX', 'ADBE', 'CRM',
        'AMD', 'INTC', 'PYPL', 'SQ', 'SHOP'
    ]

    start_date = '2018-01-01'
    end_date = '2025-10-28'

    print("Downloading data...")
    raw_data = get_data(universe, start_date, end_date)

    # 转换为 Backtrader 数据
    cerebro = bt.Cerebro()
    cerebro.broker.setcash(100000.0)  # 初始资金10万美元
    cerebro.broker.setcommission(commission=0.001)  # 0.1% 手续费

    for ticker, df in raw_data.items():
        data = bt.feeds.PandasData(
            dataname=df,
            name=ticker,
            timeframe=bt.TimeFrame.Days,
            compression=1
        )
        cerebro.adddata(data)

    cerebro.addstrategy(MomentumRotationStrategy,
                        lookback=252,
                        top_n=5,
                        rs_threshold=70,
                        rebalance_day=1)

    cerebro.addanalyzer(bt.analyzers.SharpeRatio, _name='sharpe')
    cerebro.addanalyzer(bt.analyzers.DrawDown, _name='drawdown')
    cerebro.addanalyzer(bt.analyzers.Returns, _name='returns')

    print("Starting backtest...")
    results = cerebro.run()
    strat = results[0]

    # ==================== 4. 输出结果 ====================
    print("\n" + "="*50)
    print("BACKTEST RESULTS")
    print("="*50)
    print(f"Initial Capital: $100,000")
    print(f"Final Portfolio Value: ${cerebro.broker.getvalue():,.2f}")
    print(f"Total Return: {(cerebro.broker.getvalue()/100000 - 1)*100:.2f}%")
    print(f"Sharpe Ratio: {strat.analyzers.sharpe.get_analysis()['sharperatio']:.3f}")
    print(f"Max Drawdown: {strat.analyzers.drawdown.get_analysis()['max']['drawdown']:.2f}%")

    # ==================== 5. 绘图 ====================
    plt.figure(figsize=(12, 8))
    cerebro.plot(style='candlestick')
    plt.title("Momentum Rotation Strategy - Equity Curve")
    plt.show()