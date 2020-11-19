import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd

result_file = '/var/qindom/foodhwy/data_file/results_file_624.csv'
df = pd.read_csv(result_file)

df['xgb_error_time'] = abs(df['trip_time'] - df['xgb_pred_time'])
# df['fml_error_time'] = abs(df['trip_time'] - df['fml_pred_time'])

# df['our model error distribution'] = pd.Series([])
# df.loc[df['xgb_error_time'] <= 1, 'our model error distribution'] = "error_less_than_1min"
# df.loc[(df['xgb_error_time'] > 1) & (df['xgb_error_time'] <= 2), 'our model error distribution'] = "error_less_than_2min"
# df.loc[(df['xgb_error_time'] > 2) & (df['xgb_error_time'] <= 5), 'our model error distribution'] = "error_less_than_5min"
# df.loc[df['xgb_error_time'] > 5, 'our model error distribution'] = "error_more_than_5min"
#
# df['formula error distribution'] = pd.Series([])
# df.loc[df['fml_error_time'] <= 1, 'formula error distribution'] = "error_less_than_1min"
# df.loc[(df['fml_error_time'] > 1) & (df['fml_error_time'] <= 2), 'formula error distribution'] = "error_less_than_2min"
# df.loc[(df['fml_error_time'] > 2) & (df['fml_error_time'] <= 5), 'formula error distribution'] = "error_less_than_5min"
# df.loc[df['fml_error_time'] > 5, 'formula error distribution'] = "error_more_than_5min"
#
# df_test = pd.DataFrame([])
# df_test['trip_time'] = pd.concat([df['trip_time'], df['trip_time']])
# df_test['pred_time'] = pd.concat([df['xgb_pred_time'], df['fml_pred_time']])
# df_test['error_distribution'] = pd.concat([df['our model error distribution'], df['formula error distribution']])
# df_test.index = range(len(df_test))
# index = int(len(df) - 1)
# df_test.loc[: index, 'model'] = 'our model'
# df_test.loc[index + 1:, 'model'] = 'traditional formula'

sns.set(color_codes=True)

# sns.jointplot(x="xgb_pred_time", y="trip_time", data=df)
# sns.jointplot(x="fml_pred_time", y="trip_time", data=df)


markers = {"error_less_than_1min": "o", "error_less_than_2min": "P", "error_less_than_5min": "d",
           "error_more_than_5min": "<"}

# plot fml time distribution
# sns.scatterplot('fml_pred_time', 'trip_time', data=df, markers=markers, style="formula error distribution",
#                 hue="formula error distribution",
#                 palette=dict(error_less_than_1min="#9b59b6", error_less_than_2min="#3498db",
#                              error_less_than_5min="#2ecc71", error_more_than_5min="#e74c3c"))

# plot cgb time distribution
# sns.scatterplot('xgb_pred_time', 'trip_time', data=df, markers=markers, hue="our model error distribution",
#                 style="our model error distribution",
#                 palette=dict(error_less_than_1min="#9b59b6", error_less_than_2min="#3498db",
#                              error_less_than_5min="#2ecc71", error_more_than_5min="#e74c3c"))

# plt.xlabel("formula predict time (unit: minute)")
# plt.ylabel("google maps trip time (unit: minute)")


df['our model error distribution'] = pd.Series([])

df.loc[df['xgb_error_time'] <= 1, 'our model error distribution'] = "error less than 1min"
df.loc[(df['xgb_error_time'] > 1) & (df['xgb_error_time'] <= 2), 'our model error distribution'] = "error less than 2min"
df.loc[(df['xgb_error_time'] > 2) & (df['xgb_error_time'] <= 5), 'our model error distribution'] = "error less than 5min"
df.loc[df['xgb_error_time'] > 5, 'our model error distribution'] = "error more than 5min"

# df['formula error distribution'] = pd.Series([])
# df.loc[df['fml_error_time'] <= 1, 'formula error distribution'] = "error less than 1min"
# df.loc[(df['fml_error_time'] > 1) & (df['fml_error_time'] <= 2), 'formula error distribution'] = "error less than 2min"
# df.loc[(df['fml_error_time'] > 2) & (df['fml_error_time'] <= 5), 'formula error distribution'] = "error less than 5min"
# df.loc[df['fml_error_time'] > 5, 'formula error distribution'] = "error more than 5min"

df_test = pd.DataFrame([])
df_test['trip_time'] = pd.concat([df['trip_time'], df['trip_time']])
df_test['pred_time'] = pd.concat([df['xgb_pred_time'], df['fml_pred_time']])
df_test['error_distribution'] = pd.concat([df['our model error distribution'], df['formula error distribution']])
df_test.index = range(len(df_test))
index = int(len(df) - 1)
df_test.loc[: index, 'model'] = 'our model'
df_test.loc[index + 1:, 'model'] = 'traditional formula'


df_bar = df_test.groupby(['error_distribution', 'model']).count()
df_bar.reset_index(level=0, inplace=True)
df_bar.reset_index(level=0, inplace=True)
print(df_bar)
df_bar = df_bar[['error_distribution','model','trip_time']]
df_bar.rename(columns={'trip_time': "number of trips"}, inplace=True)
print(df_bar)
ax = sns.barplot(x="error_distribution", y="number of trips", hue="model", data=df_bar)



'''
df_bar = df_test.groupby(['error_distribution', 'model']).count()
df_bar.reset_index(level=0, inplace=True)
df_bar.reset_index(level=0, inplace=True)
print(df_bar)
df_bar = df_bar[['error_distribution', 'model', 'trip_time']]
df_xgb = df_bar[df_bar['model'] == 'qmodel']
df_fml = df_bar[df_bar['model'] == 'traditional formula']
print(df_xgb)
print(df_fml)
# a = float(84894)
'''

plt.show()
