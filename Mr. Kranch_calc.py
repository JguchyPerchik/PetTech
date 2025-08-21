import pandas as pd
from IPython.display import display

# --- Правила формирования блоков ---
block_rules = {
    "Discovery": {"toys": 25, "bowls": 15},
    "Happy Launch": {"toys": 40, "bowls": 0},
    "Play": {"toys": 20, "bowls": 0},
}

# --- Валидация входных данных ---
def validate_dataframe(df: pd.DataFrame):
    required_columns = {"Manager", "Contract_ID", "Order", "Category", "Nomenclature_ID", "Quantity", "Amount", "Price"}
    missing = required_columns - set(df.columns)
    if missing:
        raise ValueError(f"Отсутствуют обязательные колонки: {missing}")

    if not pd.api.types.is_numeric_dtype(df["Quantity"]):
        raise TypeError("Колонка 'Quantity' должна быть числовой")
    if not pd.api.types.is_numeric_dtype(df["Amount"]):
        raise TypeError("Колонка 'Amount' должна быть числовой")
    if not pd.api.types.is_numeric_dtype(df["Price"]):
        raise TypeError("Колонка 'Price' должна быть числовой")


# --- Функция для сборки конкретного блока ---
def assemble_block(order_df, used_ids, included_rows, needed_toys, needed_bowls):
    toys = order_df[(order_df['Category'] == 'Игрушки') & (~order_df['Nomenclature_ID'].isin(used_ids))]
    bowls = order_df[(order_df['Category'] == 'Миски') & (~order_df['Nomenclature_ID'].isin(used_ids))]

    toys_qty = toys['Nomenclature_ID'].nunique()
    bowls_qty = bowls['Nomenclature_ID'].nunique()

    # сколько блоков можно собрать
    if needed_toys > 0 and needed_bowls > 0:
        k = min(toys_qty // needed_toys, bowls_qty // needed_bowls)
    else:
        k = toys_qty // needed_toys if needed_toys > 0 else 0

    if k > 0:
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


# --- Основная функция подсчёта блоков ---
def count_brand_blocks(order_df):
    order_df = order_df.sort_values(by='Price', ascending=False)
    used_ids = set()
    included_rows = []
    blocks = {}

    for block_name, rules in block_rules.items():
        blocks[block_name] = assemble_block(
            order_df,
            used_ids,
            included_rows,
            needed_toys=rules["toys"],
            needed_bowls=rules["bowls"]
        )

    return pd.Series({**blocks, 'included_idx': included_rows})


# --- Основной pipeline ---
def process_file(input_path="src/Mr. Kranch.xlsx", output_path="Mr. Kranch_calc.xlsx"):
    df = pd.read_excel(input_path)
    df.columns = df.columns.str.strip()

    # Нормализация чисел
    for col in ["Amount", "Price"]:
        df[col] = df[col].astype(str).str.replace(',', '.').astype(float)

    validate_dataframe(df)

    writer = pd.ExcelWriter(output_path, engine="xlsxwriter")

    # Подсчёт блоков
    order_blocks = df.groupby(['Manager', 'Contract_ID', 'Order']).apply(count_brand_blocks).reset_index()

    included_indices = set()
    for idx_list in order_blocks['included_idx']:
        included_indices.update(idx_list)

    df_with_blocks = df.loc[df.index.isin(included_indices)]

    order_blocks["Total_blocks"] = order_blocks[list(block_rules.keys())].sum(axis=1)

    summary = df_with_blocks.groupby("Manager").agg(
        Contracts_with_blocks=("Contract_ID", "nunique"),
        Sales_sum=("Amount", "sum")
    ).reset_index()

    blocks_summary = order_blocks.groupby("Manager")["Total_blocks"].sum().reset_index()
    summary = summary.merge(blocks_summary, on="Manager", how="left")

    order_blocks.to_excel(writer, sheet_name="full")
    summary.to_excel(writer, sheet_name="summary")

    ranking_table = make_ranking(summary)
    display(ranking_table)
    ranking_table.to_excel(writer, index=False, sheet_name="ranking")
    writer.close()


# --- Рейтинг ---
def make_ranking(summary):
    ranking = summary.copy()
    ranking["Rank_blocks"] = ranking["Total_blocks"].rank(method="min", ascending=False).astype(int)
    ranking["Rank_contracts"] = ranking["Contracts_with_blocks"].rank(method="min", ascending=False).astype(int)
    ranking["Rank_sales"] = ranking["Sales_sum"].rank(method="min", ascending=False).astype(int)

    ranking["Total_score"] = ranking["Rank_blocks"] + ranking["Rank_contracts"] + ranking["Rank_sales"]
    ranking = ranking.sort_values(by="Total_score")
    return ranking.reset_index(drop=True)


# --- Запуск ---
if __name__ == "__main__":
    process_file()
