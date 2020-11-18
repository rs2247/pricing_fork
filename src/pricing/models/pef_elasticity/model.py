# import time
# import math
# import numpy as np
# import scipy.linalg as linalg
# import matplotlib.pyplot as plt
# import pandas as pd
# import string
# import seaborn as sns
# import sklearn

# import matplotlib.gridspec as gridspec
# from sklearn.linear_model import LogisticRegression
# from sklearn.metrics import precision_recall_curve
# from sklearn.metrics import roc_auc_score, auc, roc_curve, brier_score_loss, average_precision_score
# from sklearn.model_selection import KFold
# import sklearn

# from scipy import interp

# from sklearn.model_selection import cross_validate

# from xgboost import XGBClassifier, XGBRegressor
# from xgboost import plot_importance
# import xgboost as xgb



# # def train_predict_split(df_spine, revenue_cut_percentile):

# # 	df_spine = df_spine.reset_index(drop=True)

# # 	target_cut = get_target_cut(df_spine,revenue_cut_percentile)
# # 	print("Total revenue cut defining our classes (to be in the ",100*revenue_cut_percentile,"th percentile of revenue): ", target_cut)

# # 	#We wanna excluded from the training target for which we have NO OFFERS!
# # 	df_spine['predict'] = (
# # 		((df_spine['n_av_pages'].round()==0) | (df_spine['n_av_offers'].round()==0))&\
# # 		(df_spine['total_revenue']==0)
# # 	)
# # 	df_spine['avg(nu_idade)'] = df_spine['avg(nu_idade)'].astype(float)

# # 	#Splitting in master/predict
# # 	TRAIN_IGNORE_COLUMNS =  [
# # 		'predict',
# # 		'city',
# # 		'state',
# # 		'clean_canonical_course_id',
# # 		'clean_canonical_course_name',
# # 		'price_range',
# # 		'interest',
# # 		'total_revenue',
# # 		'#payments',
# # 		'n_av_offers',
# # 		'n_av_pages'
# # 	]

# # 	#Predict
# # 	df_predict = df_spine[df_spine['predict']==1].copy()
# # 	assert df_predict['total_revenue'].sum() == 0

# # 	#Master
# # 	# df_master = df_spine[df_spine['predict']==0].drop(TRAIN_IGNORE_COLUMNS,axis=1).copy()
# # 	df_master = df_spine[df_spine['predict']==0].copy()

# # 	df_master['target'] = (df_spine['total_revenue']>target_cut).astype(int)

# # 	# feature_names = df_master.columns.values[:-1]
# # 	feature_names = list(set(df_spine.columns.tolist()) - set(TRAIN_IGNORE_COLUMNS))

# # 	print("\nImbalance (%):\n" ,df_master['target'].value_counts()/df_master.shape[0])
# # 	print("\n#features: ", len(feature_names))
# # 	print("\n% of interest targets in df_predict ", 100*df_predict['interest'].sum()/df_spine['interest'].sum(), "%")

# # 	return df_master,df_predict,feature_names


# # def get_target_cut(df_spine,revenue_cut_percentile):
# # 	df_spine = df_spine.sort_values('total_revenue',ascending=False)
# # 	df_spine['cum_percentile_revenue'] = df_spine['total_revenue'].cumsum()/df_spine['total_revenue'].sum()
# # 	return df_spine[df_spine['cum_percentile_revenue']<revenue_cut_percentile].iloc[-1]['total_revenue']

# def modelfit(trainer,
# 			 X,Y,
# 			 feature_names,
# 			 roc_ax=None,
# 		     feat_imp_plot=True,
# 		     feature_importances_show=[],
# 		     return_model = True):

# 	#5-fold cross validation
# 	cv = KFold(n_splits=5,shuffle=True)

# 	mean_fpr =np.linspace(0,1,100) #x points of the ROC curve
# 	tprs={"train":[],"validation":[]}
# 	aucs={"train":[],"validation":[]}
# 	briers = {"train":[],"validation":[]}
# 	avps ={"train":[],"validation":[]} #average_precision_score (summarizes AU precision-recall curve)
# 	j=0

# 	# Using 5-fold cross-validation
# 	for train,validation in cv.split(X,Y):
# 		j=j+1
# 		print("\nFold : ", j)

# 		X_train = X[train]
# 		Y_train = Y[train]

# 		X_validation = X[validation]
# 		Y_validation = Y[validation]

# 		trainer.fit(X_train,Y_train)

# 		datasets={
# 			"train":(X_train,Y_train),
# 			"validation":(X_validation,Y_validation)
# 		}

# 		#evaluating AUC for train validation in this fold
# 		for st in datasets:

# 			(X_st,Y_st) = datasets[st]

# 			predictions_proba= trainer.predict_proba(X_st)

# 			aux = predictions_proba[:,1]
# 			# print("Predictions proba")
# 			# print(np.sort(aux)[::-1])

# 			#ROC curve

# 			fpr, tpr, _ = roc_curve(Y_st, predictions_proba[:,1])
# 			tprs[st].append(interp(mean_fpr,fpr,tpr)) #interpolating our curve to get the curve for the points
# 													  #in mean_fpr (in order to have the same x points for
# 													  #all folds)
# 			tprs[st][-1][0] =0

# 			#ROC AUC
# 			# roc_auc = auc(fpr, tpr)
# 			aucs[st].append(auc(fpr, tpr))


# 		print("%d AUC-> T: %.4f | V: %.4f  "
# 			  "%%Imbalance: -> T: %.2f | V: %.2f " % (
# 		  		j,
# 			  	aucs["train"][-1],
# 			  	aucs["validation"][-1],
# 			  	datasets["train"][1].sum()/datasets["train"][1].shape[0],
# 			  	datasets["validation"][1].sum()/datasets["validation"][1].shape[0]
# 			  	)
# 			  )



# 	color={"train":"darkorange","validation":"r"}

# 	for st in tprs:
# 		mean_tpr = np.mean(tprs[st], axis=0)
# 		mean_tpr[-1] = 1.0
# 		mean_auc = auc(mean_fpr, mean_tpr)
# 		std_auc = np.std(aucs[st])

# 		if roc_ax!=None:
# 			roc_ax.plot(mean_fpr, mean_tpr, color=color[st],
# 			 	 	 label=r'Mean ROC (AUC = %0.2f $\pm$ %0.2f)' % (mean_auc, std_auc),
# 			     	 lw=2)


# 	if roc_ax!=None:
# 		roc_ax.plot([0, 1], [0, 1], color='navy', lw=2, linestyle='--')
# 		roc_ax.set_xlim(0.0, 1.0)
# 		roc_ax.set_ylim([0.0, 1.05])
# 		roc_ax.set_xlabel('False Positive Rate')
# 		roc_ax.set_ylabel('True Positive Rate')
# 		roc_ax.set_title('Receiver operating characteristic example')
# 		roc_ax.legend(loc="lower right")


# 	#feature importances (just for xgb)
# 	if isinstance(trainer,XGBClassifier):

# 		#Using "gain" as score of feature_importance:
# 		df = get_xgb_feat_importances(trainer,feature_names,importance_type = "gain")

# 		top_features = df["Feature"].values[:10]
# 		importances  = df["Importance"].values[:10]

# 		#Showing importance of required features
# 		if feature_importances_show:
# 			print("\nSelected feature Importances ")
# 			for feature in feature_importances_show:
# 				if feature in df["Feature"].values:
# 					print ("Feature: %s -> %f" %(feature,df.loc[df["Feature"]==feature,"Importance"].values[0]))
# 				else:
# 					print ("Feature: %s not used for prediction"%(feature))


# 		#Plotting feature importance for the last cv run
# 		if feat_imp_plot:
# 			fig,ax = plt.subplots(1,1)

# 			n_points = min(len(importances),10)
# 			ax.set_title("Feature importances")
# 			ax.barh(np.arange(n_points), importances, color="b", align="center")
# 			ax.set_ylim(-1,n_points)
# 			ax.invert_yaxis()
# 			ax.set_yticks(np.arange(n_points))
# 			ax.set_yticklabels(top_features)



# 	if return_model:
# 		return trainer

# 	else:
# 		return (np.mean(aucs["validation"]),np.std(aucs["validation"]),(Y_validation,predictions_proba[:,1]))


# '''
# 	Returns feature importances for all features used in a
# 	XGBClassifier, using inputed importance_type

# 	-clf: xgb.XGBModel
# 	-feature_name: list
# 	-importance_type: same as xgboost docs
# '''


# def get_xgb_feat_importances(clf,feature_names,importance_type):

#     if isinstance(clf, xgb.XGBModel):
#         # clf has been created by calling
#         # xgb.XGBClassifier.fit() or xgb.XGBRegressor().fit()
#         fscore = clf.get_booster().get_score(importance_type=importance_type)
#     else:
#         # clf has been created by calling xgb.train.
#         # Thus, clf is an instance of xgb.Booster.
#         fscore = clf.get_fscore()

#     feat_importances = []


#     # for ft, score in fscore.iteritems():
#     for ft, score in iter(fscore.items()):
#         # feat_importances.append({'Feature': ft, 'Importance': score})
#         feat_importances.append({'Feature': feature_names[int(ft.replace("f",""))], 'Importance':score})

#     feat_importances = pd.DataFrame(feat_importances)
#     feat_importances = feat_importances.sort_values(
#         by='Importance', ascending=False).reset_index(drop=True)
#     # Divide the importances by the sum of all importances
#     # to get relative importances. By using relative importances
#     # the sum of all importances will equal to 1, i.e.,
#     # np.sum(feat_importances['importance']) == 1
#     feat_importances['Importance'] /= feat_importances['Importance'].sum()
#     # Print the most important features and their importances
#     # print feat_importances.head()
#     return feat_importances
