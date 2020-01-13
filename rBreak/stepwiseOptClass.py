from vnpy.trader.utils import optimize
from datetime import datetime
import pandas as pd
import os

class stepwiseOpt:
    # 需要输入策略的类名，引擎的设置，优化目标，优化任务，全局设置，输出的地址(输出地址不输会在当前目录)
    def __init__(self, strategyClass, engineSetting, optTarget, optTask, globalSetting, outFile=None, root=None):
        self.symbol = engineSetting['symbolList'][0]
        self.strategyClass = strategyClass
        self.engineSetting = engineSetting
        self.optTarget = optTarget
        self.optTask = optTask
        self.globalSetting = globalSetting
        self.outFile = outFile

    def initOptimize(self, paramSetting, root=None):
        optimize.strategyClass = self.strategyClass
        optimize.engineSetting = self.engineSetting
        optimize.globalSetting = self.globalSetting
        optimize.paramsSetting = paramSetting
        optimize.root = root
        optimize.initOpt()

    # ----所有择优方法返回一个参数名对应最优参数的字典----
    def pick_best_param(self, report, params):
        # 返回key为参数名， value为参数值的list列表
        bestParamsDict = {paramName : report.iloc[0][paramName] for paramName in params}
        return bestParamsDict

    def pick_freq_param(self, report, params, n=13):
        # 返回key为参数名， value为参数值的list列表
        # 取第一个key作为次数最多的选择
        reportHead = report.head(n)
        firstParams = list(params.keys())[0]
        # 排除只有一个结果，并判断前两个结果的频次是否一样
        try:
            sameResult = reportHead[firstParams].value_counts().iloc[0]==reportHead[firstParams].value_counts().iloc[1]
        except IndexError:
            sameResult = False
        # 如果有一样的将n整除2取出现次数最多的        
        freqResult = None
        if sameResult:
            reportHead = report.head(n//2)
            freqResult = reportHead[firstParams].value_counts().index[0]
        else:
            freqResult = reportHead[firstParams].value_counts().index[0]
        freqReport = reportHead[reportHead[firstParams]==freqResult]
        bestParamsDict = {paramName : freqReport.iloc[0][paramName] for paramName in params}
        return bestParamsDict

    def optFunc(self, method, report, params):
        funcDict = {
                    "pick_best_param": self.pick_best_param(report, params),
                    'pick_freq_param': self.pick_freq_param(report, params)
                   }
        return funcDict[method]

    #----保存可设置为创建文件夹---
    def makeFolder(self, startTime):
        # 用不同的时间区分不同的优化结果
        if not self.outFile:
            folder = 'opt_'
        else:
            folder = self.outFile+'/opt_'
        
        folder = folder + self.strategyClass.className + self.symbol.replace(':', '')
        folder = folder+startTime.strftime("%m%d%H%M%S")  # 用不同的时间区分不同的优化结果
        return folder

    # 创建一个文件夹用于存储结果
    def makefile(self, folder):
        if not self.outFile:
            # get current workding dir
            path = os.getcwd()
            if os.path.exists(os.path.join(path, folder)):
                print(f'该文件夹内{folder}文件已存在，继续添加')
            else:
                os.makedirs(os.path.join(path, folder))
        else:
            ### 先创建文件夹
            if os.path.exists(self.outFile):
                print(f'{self.outFile}已存在，在该文件夹内继续添加新结果')
            else:
                os.makedirs(self.outFile)    
            ### 在文件夹下继续按照给定的命名folder创建文件
            if not os.path.exists(os.path.join(self.outFile,folder)):
                os.makedirs(os.path.join(self.outFile,folder))

    def savePerformance(self, report, method, idx, startTime):
        symbol = self.symbol.replace(':','_')
        folder = self.makeFolder(startTime)
        self.makefile(folder)
        report.to_csv(f"{folder}\opt_{method}_{idx}.csv")
        return report

    def runMemoryParallel(self):
        start = datetime.now()
        print("run memory | start: %s -------------------------------------------" % start)
        pre_params = {}
        for idx, setting in enumerate(self.optTask):
            for method, params in setting.items():
                params.update(pre_params)
                # self.initOptimize(params, f"opt_memory_{idx}")
                self.initOptimize(params)
                report = optimize.runParallel()
                report.sort_values(by = self.optTarget, ascending=False, inplace=True)
                # 保存路径设置
                report = self.savePerformance(report, method, idx, start)
                # 选取最优参数进行下一次优化
                pickDict = self.optFunc(method, report, params)
                pre_params.update(pickDict)
                
        end = datetime.now()
        print("run memory | end: %s | expire: %s -----------------------------" % (end, end - start))
