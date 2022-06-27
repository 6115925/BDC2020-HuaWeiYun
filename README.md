2020中国高校计算机大赛·华为云大数据挑战赛  

#  #一、数据处理部分代码：
	##  ##包括了从最初的去重，删除无用列，清理方向为-1或者大于36000，漂点、速度为0经纬度却在走，以及速度大于55。同条船的时间记录重复，对于很多订单重复部分占比很多，但首尾时间不同的采用人工提取出数据观察并判断是否删除，以及将测试集路由放入重新提取完整数据以及对数据路由进行重新定义，通过画图方法对训练集画图观察轨迹是否发生截断，人工清洗轨迹与测试集差异大的数据，还包括将数据同路由的路线距离进行计算，人工清洗部分距离差异大的数据。 
	##  ##代码工程：包括对于测试集、训练集特征的提取，提取的特征工程包括为：当前点的经纬度，速度，目的港口经纬度，距离目的港的位移，测试集训练集所有的船号路由标签编码；采用五折交叉验证的LGB模型
	##  ##模型读入数据：模型数据为经过对测试集特征提取后的用于预测的测试集特征文件
#  #二、项目所使用平台：
	##  ##本地Windows 10系统、阿里云16核+128GiB CentOS 7.6 64位Linux服务器

#  #三、代码运行环境：
	##  ##Python3.7版本编程语言、pandas 1.0.1、numpy1.18.1、descartes 1.1.0、Fiona 1.8.13、Gdal 3.1.2、pyproj 2.6.1、Shapely 1.7.0、geopandas0.8.1、matplotlib 3.1.3、scikit-learn 0.22.1、tqdm 4.42.1、lightgbm 2.3.0
#  #四、解题思路：
	##  ##整体分析思路：初赛采用统计特征的方法，取整条订单全部数据进行统计，同时对测试集也采用相同的统计特征方法，包括经纬度、速度、方向的均值，方差，最值等，同时也将到达目的港口的位移距离做一个特征；之后通过尝试把路由作为特征，并且通过官方训练集中的有路由的数据与测试集路由进行匹配，从中间对训练集路由包含测试集的进行截断并取位移前10%，有了一个比较好的提升。复赛时初赛方法并不适用，最初考虑是否是特征较少导致，我们通过对智慧海洋比赛的代码学习，采用了对经纬度进行GeoHash编码，构造词向量，同时对订单速度不为0以及经纬度变化的数据分别进行了统计特征，此时特征总数达到301个，然而预测效果和初赛方法相差不大。之后重新建模，采用所有数据全部放入，拿测试集订单最后一个点代入预测答案的方法有了一个较好的提升，经过分析该模型上限不高，而且重要特征路由也无法放入进行学习。之后对数据进行更进一步的清洗，以及尝试相似轨迹的等方法，形成了一个用于训练模型较为完善的数据集，以及比较适应模型的特征集。经过不断地探索，我们最后用于B阶段的特征包括：当前点的经纬度，速度，船号，路由，目的港口经纬度，距离目的港的位移。
	##  ##数据处理思路：经过对测试集分布进行分析，最初采用对训练集截取位移前10%的方法，进行模型的预测，对于异常数据，只稍微处理了difftime，diffdis过大的数据。之后便使用测试集路由放入具有路由的训练集中对数据进行重取，对于数据到港打标，初赛阶段，我们一致认为2w米且速度为0较为合适，结果经过测试发现预测效果还不如不对到港进行判断的效果好。复赛阶段对于数据去重后我们首先进行了漂点的清洗，接着再对数据进行了方向异常，速度过大的进行清洗，也清洗了训练集中部分同一条船时间相同的的数据，也通过人工几天时间的清洗对订单多条船相同，时间相同的数据和部分数据发生断层的合并等。对速度也进行了修正，将测试集的路由放进去匹配重新选取数据，并对到港打标规则进行了多种组合尝试，找到了经纬度0.25和速度为0的到港方式，并用画图的方法对数据进行补充以及清洗。
##  ##模型选型思路：
	##  ##在训练数据，数据量很大的情况下，xgboost的训练速度相较于lightgbm比较慢，所以我们还是采用lightgbm模型，5折交叉验证。
#  #五、代码说明：
	##  ##LGB模型包含数据类型转化模块，机器学习模块。其中数据类型转化模块就是将读入的数据类型转化成正确的类型，机器学习模块，通过lgb自带模块读取保存的模型，用来预测数据。
	##  ##在模型使用文件中，运行” LGB模型使用(预测)”即可对模型预测结果进行还原，生成文件在data中，测试特征集存放于new中，测试集中船号、路由的编码在训练的过程中生成。在test_data_path处设置测试集文件的路径，test_path处设置测试集特征文件，result.to_csv处设置 预测生成文件路径，在lgb.Booster（）内输入model_file的文件路径。
