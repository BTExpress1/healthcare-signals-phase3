import matplotlib.pyplot as plt

def plot_provider_trend(df, value_col):
    plt.figure(figsize=(12,4))
    plt.plot(df['snapshot_dt'], df[value_col], marker='o')
    plt.title(f"Provider {df.provider_id.iloc[0]} — {value_col}")
    plt.xlabel("Month")
    plt.ylabel(value_col)
    plt.grid(True)
    plt.show()
