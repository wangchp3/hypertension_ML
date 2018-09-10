# encoding utf-8
# from test
# 预处理
# 针对数据维度上进行降维操作
import matplotlib.pyplot as plt
import numpy as np
from sklearn import decomposition


class DataPCA:
    def __init__(self, *data):
        self.X, self.y = data

    def data_pca(self, n_components=None):
        """
        对数据进行降维，降维方法是主成分分析法，确定维度是几维能够解释基本变差
        :param n_components:None 则选取他的指为min(n_samples,n_features)；'mle'则使用Minka MLE算法来猜测降维后的维数；若0，1之间的浮点数，则是指百分比。
        :return:
        """
        pca = decomposition.PCA(n_components)
        pca.fit(self.X)
        print('explained variance ratio:%s' % str(pca.explained_variance_ratio_))
        return pca.components_, pca.explained_variance_ratio_

    def plot_pca(self, n_components=None):
        """
        对数据进行降维，并对降维的数据进行画图，有上可得的维数
        :param n_components: n_components:None 则选取他的指为min(n_samples,n_features)；'mle'则使用Minka MLE算法来猜测降维后的维数；若0，1之间的浮点数，则是指百分比。
        :return:
        """
        pca = decomposition.PCA(n_components)
        pca.fit(self.X)
        X_r = pca.transform(self.X)

        fig = plt.figure()
        ax = fig.add_subplot(1, 1, 1)
        colors = (
            (1, 0, 0), (0, 1, 0), (0, 0, 1), (0.5, 0.5, 0), (0, 0.5, 0.5), (0.5, 0, 0.5), (0.4, 0.6, 0), (0.4, 0, 0.6),
            (0.6, 0, 0.4), (0, 0.6, 0.4), (0, 0.4, 0.6), (0.5, 0.3, 0.2), (0.5, 0.3, 0.2), (0.3, 0.2, 0.5))
        for label, color in zip(np.unique(self.y), colors):
            position = self.y == label
            ax.scatter(X_r[position, 0], X_r[position, 1], label="target=%d" % label, color=color)

        ax.set_xlabel("X[0]")
        ax.set_ylabel("Y[0]")
        ax.legend(loc="best")
        ax.set_title("PCA Result")
        plt.show()
