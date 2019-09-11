import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
plt.style.use('seaborn')

# Specify directory
dir_path = './data/extracted/'
train_path = dir_path + 'train.csv'

# Read data
df = pd.read_csv(train_path, nrows=1e7)

# Basic info
df.info()

df.head(30)

# Plot
fig, ax = plt.subplots(2, 1)
ax[0].plot(df['acoustic_data'], label='acoustic')
ax[0].legend()
ax[1].plot(df['time_to_failure'], c='g', label='time to failure')
ax[1].legend()
plt.show()
