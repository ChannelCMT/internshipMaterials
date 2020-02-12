from stepwiseOptClass import stepwiseOpt
import numpy as np
import json
import os
from datetime import datetime
from rBreakStrategy import rBreakStrategy

STRATEGYCLASS = rBreakStrategy

# 指定引擎设置
ENGINESETTING = {
                "timeRange": {
                    "tradeStart":datetime(2018, 6, 1),
                    "tradeEnd":datetime(2019, 7, 1),
                    "historyStart":datetime(2018, 1, 3)
                    },
                "contracts":[{
                            "slippage": 0.005,
                            "rate": 0.0005,
                            "symbol": "btc_usd_cq.future:okex"
                           }],
                'dbURI': "mongodb://172.16.11.81:27017",
                "bardbName": "Kline_1Min_Auto_Db_Plus",
                "symbolList": ["btc_usd_cq.future:okex"]
                }


# 优化目标
OPT_TARGET = "sharpeRatio"
# np.arange(0.1,0.2,0.01)
# range(1,10,1)
# 指定优化任务
OPT_TASK = [
            {"pick_freq_param": 
                {
                "rangePeriod": range(200, 241, 20),
                "breakPct": np.arange(0.35, 0.61, 0.01)
                }
            }, 
            {"pick_best_param": 
                {
                "observedPct": np.arange(0.2, 0.41, 0.01),
                "reversedPct": np.arange(0.01,0.22,0.01),
                }
            }, 
            {"pick_best_param": 
                {
                "signalPeriod": range(4, 61, 1)
                }
            }, 
            {"pick_best_param": 
                {
                "dailyPeriod": [24, 12, 8, 6],
                "calTime": [20, 0, 14, 8]
                }
            }, 
]

def main():
    path = os.path.split(os.path.realpath(__file__))[0]
    with open(path+"//CTA_setting.json") as f:
        globalSetting = json.load(f)[0]

    globalSetting.update(ENGINESETTING)
    optimizer = stepwiseOpt(STRATEGYCLASS, ENGINESETTING, OPT_TARGET, OPT_TASK, globalSetting, '../optResult')
    optimizer.runMemoryParallel()

if __name__ == '__main__':
    main()