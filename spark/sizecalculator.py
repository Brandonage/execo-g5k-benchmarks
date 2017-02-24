### THIS IS AN SMALL UTIL TO CALCULATE THE NUMBER OF POINTS WE NEED TO GENERATE A FILE OF SIZE X

def get_npoints_for_size(app,size):
    refpoints = 1000000 ## This are the reference number of points we used to infer the ratios in refsizedict
    refsizedict = {"words": 0.001, "svm": 0.192, "kmeans":0.372, "logistic":0.379, "linear":0.379, "pca":18.4,"decisiontree":0.120,
                   "relational":0.124,"graph":1.9}
    refsize = refsizedict[app]
    npoints = (size*refpoints)/refsize
    return str(int(npoints))