from stepwiseOptClass import stepwiseOpt
import numpy as np
import json
import os
from datetime import datetime
from kamaSarStrategy import kamaSarStrategy

STRATEGYCLASS = kamaSarStrategy

# 指定引擎设置
ENGINESETTING = {
                "timeRange": {
                    "tradeStart":datetime(2014, 3, 1),
                    "tradeEnd":datetime(2017, 12, 30),
                    "historyStart":datetime(2014, 1, 1)
                    },
                "contracts":[{
                            "slippage": 0,
                            "rate": 0.0008,
                            "symbol": "IF:CTP"
                           }],
                'dbURI': "mongodb://172.16.11.81:27017",
                "bardbName": "VnTrader_1Min_Db_contest",
                "symbolList": ["IF:CTP"]
                }


# 优化目标
OPT_TARGET = "sharpeRatio"
# np.arange(0.1,0.2,0.01)
# range(1,10,1)
# 指定优化任务
OPT_TASK = [
            {"pick_freq_param": 
                {
                "rangePeriod": range(4,49,4),
                "breakPct": np.arange(0.1, 0.41, 0.05)
                }
            }, 
            {"pick_best_param": 
                {
                "observedPct": np.arange(0.1, 0.41, 0.05),
                "reversedPct": np.arange(0.03,0.22,0.03),
                }
            }, 
            {"pick_best_param": 
                {
                "signalPeriod": range(4, 61, 4)
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