#! /usr/bin/env python  
# -*- coding:utf-8 -*-
import numpy as np


class KNN:

    def __init__(self):
        pass

    def create_dataset(self):
        group = np.array([[1.0, 1.1], [1.0, 1.0], [0, 0], [0, 0.1]])
        labels = ['A', 'A', 'B', 'B']
        return group, labels

    """使用欧氏距离计算前K个近邻，并分类，算法公式是(x1-x2)**2"""
    def classfy0(self, inX, dataSet, labels, k):
        # 求数据集行数
        dataSetSize = dataSet.shape[0]
        # 扩展测试数据，使其与训练数据大小一致,相减得到(x1-x2)
        diffMat = np.tile(inX, (dataSetSize, 1)) - dataSet
        sqdiffMat = diffMat ** 2
        # axis=0列 axis=1行
        sqDistances = sqdiffMat.sum(axis=1)
        # 开根号两种写法都行np.sqrt(sqDistances)    sqDistances ** 0.5
        distances = sqDistances ** 0.5
        # 对距离排序，默认从小到大两种写法np.argsort(distances)  distances.argsort()
        # 返回是所有从小到大的数值的下标如0,3,2,1
        sortedDistances = distances.argsort()
        classCount = {}
        # 从所有排序数据中取出前K个最近距离，记录下标序号，按出现下标序号来分类统计出现的总次数，再排序
        for i in range(k):
            voteLabels = labels[sortedDistances[i]]
            classCount[voteLabels] = classCount.get(voteLabels, 0)+1
        print(classCount)
        # 对最终结果从大到小排序
        sortedClassCount = sorted(classCount.items(), key=lambda classCount:classCount[1], reverse=True)
        print(sortedClassCount)
        # 取第一个最接近的分类
        return sortedClassCount[0][0]


if __name__ == "__main__":
    knn = KNN()
    group, labels = knn.create_dataset()
    maxCoutClassValue = knn.classfy0([0, 0.2], group, labels, 2)
    print(maxCoutClassValue)


