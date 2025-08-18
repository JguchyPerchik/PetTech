import pandas as pd
from IPython.display import display

# Загружаем файл
df = pd.read_excel("Mr. Kranch.xlsx")
df.columns = df.columns.str.strip()

# Нормализуем числовые поля
df['Amount'] = df['Amount'].astype(str).str.replace(',', '.').astype(float)
df['Price'] = df['Price'].astype(str).str.replace(',', '.').astype(float)

def count_brand_blocks(order_df):
    order_df = order_df.sort_values(by='Price', ascending=False)
    blocks = {'Play': 0, 'Happy Launch': 0, 'Discovery': 0}
    used_ids = set()  # SKU, уже использованные в блоках
    included_rows = []  # для последующего суммирования продаж

    # Функция для "сборки" блока
    def take_block(category_filter, needed_toys=0, needed_bowls=0):
        nonlocal used_ids, included_rows

        toys = order_df[(order_df['Category'] == 'Игрушки') & (~order_df['Nomenclature_ID'].isin(used_ids))]
        bowls = order_df[(order_df['Category'] == 'Миски') & (~order_df['Nomenclature_ID'].isin(used_ids))]

        toys_qty = toys.groupby('Nomenclature_ID')['Quantity'].sum().sum()
        bowls_qty = bowls.groupby('Nomenclature_ID')['Quantity'].sum().sum()

        # сколько блоков можно собрать
        if needed_toys > 0 and needed_bowls > 0:
            k = min(toys_qty // needed_toys, bowls_qty // needed_bowls)
        else:
            k = toys_qty // needed_toys if needed_toys > 0 else 0

        if k > 0:
            # Определяем, какие SKU пойдут в блок
            toys_needed_total = k * needed_toys
            bowls_needed_total = k * needed_bowls

            if needed_toys > 0:
                for idx, row in toys.iterrows():
                    if toys_needed_total <= 0:
                        break
                    take_qty = min(row['Quantity'], toys_needed_total)
                    toys_needed_total -= take_qty
                    included_rows.append(idx)
                    used_ids.add(row['Nomenclature_ID'])

            if needed_bowls > 0:
                for idx, row in bowls.iterrows():
                    if bowls_needed_total <= 0:
                        break
                    take_qty = min(row['Quantity'], bowls_needed_total)
                    bowls_needed_total -= take_qty
                    included_rows.append(idx)
                    used_ids.add(row['Nomenclature_ID'])

        return k

    # 1. Discovery: 25 игрушек + 15 мисок
    blocks['Discovery'] = take_block(['Игрушки', 'Миски'], needed_toys=25, needed_bowls=15)

    # 2. Happy Launch: 40 игрушек
    blocks['Happy Launch'] = take_block(['Игрушки'], needed_toys=40)

    # 3. Play: 20 игрушек
    blocks['Play'] = take_block(['Игрушки'], needed_toys=20)

    return pd.Series({**blocks, 'included_idx': included_rows})

# Считаем блоки по заказам
order_blocks = df.groupby(['Manager', 'Contract_ID', 'Order']).apply(count_brand_blocks).reset_index()

# Достаём индексы строк, которые входят в бренд-блоки
included_indices = set()
for idx_list in order_blocks['included_idx']:
    included_indices.update(idx_list)

# Фильтруем только строки, входящие в блоки
df_with_blocks = df.loc[df.index.isin(included_indices)]

# Добавляем столбец с общим числом бренд-блоков в order_blocks
order_blocks["Total_blocks"] = (
    order_blocks["Play"] + order_blocks["Happy Launch"] + order_blocks["Discovery"]
)

# Считаем сводку
summary = df_with_blocks.groupby("Manager").agg(
    Contracts_with_blocks=("Contract_ID", "nunique"),
    Sales_sum=("Amount", "sum")
).reset_index()

# Добавляем количество бренд-блоков
blocks_summary = order_blocks.groupby("Manager")["Total_blocks"].sum().reset_index()

# Объединяем
summary = summary.merge(blocks_summary, on="Manager", how="left")
order_blocks.to_excel("Mr. Kranch_full.xlsx")
summary.to_excel("Mr. Kranch_calc.xlsx")
def make_ranking(summary):
    ranking = summary.copy()

    # Для каждой метрики считаем места (1 = лучший результат)
    ranking["Rank_blocks"] = ranking["Total_blocks"].rank(method="min", ascending=False).astype(int)
    ranking["Rank_contracts"] = ranking["Contracts_with_blocks"].rank(method="min", ascending=False).astype(int)
    ranking["Rank_sales"] = ranking["Sales_sum"].rank(method="min", ascending=False).astype(float)

    # Итоговый балл = сумма мест
    ranking["Total_score"] = (
        ranking["Rank_blocks"] + ranking["Rank_contracts"] + ranking["Rank_sales"]
    )

    # Сортировка по итоговому баллу
    ranking = ranking.sort_values(by="Total_score")

    return ranking.reset_index(drop=True)

# Формируем рейтинговую таблицу
ranking_table = make_ranking(summary)

display(ranking_table)
ranking_table.to_excel("Mr. Kranch_ranking.xlsx", index=False)