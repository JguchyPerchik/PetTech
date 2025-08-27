import pandas as pd
import logging
from IPython.display import display

# --- Настройка логирования ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', encoding='utf-8')

# --- Правила формирования блоков ---
block_rules = {
    "Discovery": {"toys": 25, "bowls": 15},
    "Happy Launch": {"toys": 20, "bowls": 0},
    "Play": {"toys": 40, "bowls": 0},
}

# --- Валидация входных данных ---
def validate_dataframe(df: pd.DataFrame):
    required_columns = {"Manager", "Contract_ID", "Order", "Category", "Nomenclature_ID", "Quantity", "Amount", "Price"}
    missing = required_columns - set(df.columns)
    if missing:
        logging.error(f"Отсутствуют обязательные колонки: {missing}")
        raise ValueError(f"Отсутствуют обязательные колонки: {missing}")

    if not pd.api.types.is_numeric_dtype(df["Quantity"]):
        logging.error("Колонка 'Quantity' должна быть числовой")
        raise TypeError("Колонка 'Quantity' должна быть числовой")
    if not pd.api.types.is_numeric_dtype(df["Amount"]):
        logging.error("Колонка 'Amount' должна быть числовой")
        raise TypeError("Колонка 'Amount' должна быть числовой")
    if not pd.api.types.is_numeric_dtype(df["Price"]):
        logging.error("Колонка 'Price' должна быть числовой")
        raise TypeError("Колонка 'Price' должна быть числовой")
    logging.info("Валидация данных успешно пройдена.")


# --- Функция для поиска оптимального набора блока ---
def find_block_set_details(current_inventory_df, needed_toys, needed_bowls):
    # Эта функция находит оптимальное "k" (минимальное количество единиц на SKU для блока)
    # и конкретные уникальные SKU для *одного* набора блоков, с приоритетом по количеству, затем по прибыли.
    # Она возвращает k_value, selected_skus_details (список словарей), total_profit_for_k

    current_eligible_toys = current_inventory_df[(current_inventory_df['Category'] == 'Игрушки') & (current_inventory_df['remaining_quantity'] > 0)]
    current_eligible_bowls = current_inventory_df[(current_inventory_df['Category'] == 'Миски') & (current_inventory_df['remaining_quantity'] > 0)]

    max_qty_toys = current_eligible_toys['remaining_quantity'].max() if not current_eligible_toys.empty else 0
    max_qty_bowls = current_eligible_bowls['remaining_quantity'].max() if not current_eligible_bowls.empty else 0

    max_k_possible = 0
    if needed_toys > 0 and needed_bowls > 0:
        max_k_possible = min(max_qty_toys, max_qty_bowls) if (max_qty_toys > 0 and max_qty_bowls > 0) else 0
    elif needed_toys > 0:
        max_k_possible = max_qty_toys
    elif needed_bowls > 0:
        max_k_possible = max_qty_bowls
    else:
        logging.debug("Для данного блока не требуются игрушки или миски, возвращаем 0, [], 0.0.")
        return 0, [], 0.0

    best_k_found = 0
    best_selected_skus_details = []
    max_profit_for_best_k = 0.0

    for candidate_k in range(int(max_k_possible), 0, -1):
        logging.debug(f"Попытка с candidate_k: {candidate_k}")
        temp_eligible_toys = current_eligible_toys[current_eligible_toys['remaining_quantity'] >= candidate_k]
        temp_eligible_bowls = current_eligible_bowls[current_eligible_bowls['remaining_quantity'] >= candidate_k]

        current_selected_skus = []
        current_profit = 0.0
        
        # Попытка выбрать игрушки
        if needed_toys > 0:
            if temp_eligible_toys['Nomenclature_ID'].nunique() < needed_toys:
                logging.debug(f"Недостаточно уникальных игрушек для candidate_k {candidate_k}. Продолжаем.")
                continue # Недостаточно уникальных игрушек для данного k
            
            # Выберите 'needed_toys' уникальных SKU, отдавая приоритет оставшемуся количеству, затем цене
            toys_to_consider = temp_eligible_toys.sort_values(by=['remaining_quantity', 'Price'], ascending=[False, False])
            selected_toys = toys_to_consider.drop_duplicates(subset=['Nomenclature_ID']).head(needed_toys)
            
            if len(selected_toys) < needed_toys:
                logging.debug(f"Все еще недостаточно уникальных игрушек после выбора для candidate_k {candidate_k}. Продолжаем.")
                continue # Все еще недостаточно уникальных игрушек

            for _, row in selected_toys.iterrows():
                current_selected_skus.append({
                    'original_index': row['original_index'],
                    'Nomenclature_ID': row['Nomenclature_ID'],
                    'Price': row['Price']
                })
                current_profit += row['Price'] * candidate_k

        # Попытка выбрать миски, убедившись, что нет совпадений Nomenclature_ID с выбранными игрушками
        if needed_bowls > 0:
            if temp_eligible_bowls['Nomenclature_ID'].nunique() < needed_bowls:
                logging.debug(f"Недостаточно уникальных мисок для candidate_k {candidate_k}. Продолжаем.")
                continue # Недостаточно уникальных мисок для данного k

            bowls_to_consider = temp_eligible_bowls[~temp_eligible_bowls['Nomenclature_ID'].isin([s['Nomenclature_ID'] for s in current_selected_skus])] \
                                .sort_values(by=['remaining_quantity', 'Price'], ascending=[False, False])
            selected_bowls = bowls_to_consider.drop_duplicates(subset=['Nomenclature_ID']).head(needed_bowls)

            if len(selected_bowls) < needed_bowls:
                logging.debug(f"Все еще недостаточно уникальных мисок после выбора для candidate_k {candidate_k}. Продолжаем.")
                continue # Все еще недостаточно уникальных мисок

            for _, row in selected_bowls.iterrows():
                current_selected_skus.append({
                    'original_index': row['original_index'],
                    'Nomenclature_ID': row['Nomenclature_ID'],
                    'Price': row['Price']
                })
                current_profit += row['Price'] * candidate_k
        
        # Если мы дошли сюда, значит, мы нашли действительный набор SKU для 'candidate_k'
        # Поскольку мы итерируем 'candidate_k' в обратном порядке, первое действительное 'k' является лучшим 'k'.
        logging.debug(f"Найден действительный набор SKU для candidate_k: {candidate_k}. Прибыль: {current_profit}")
        best_k_found = candidate_k
        best_selected_skus_details = current_selected_skus
        max_profit_for_best_k = current_profit
        break

    if best_k_found == 0:
        logging.debug("Не удалось найти подходящее значение k для формирования блока.")
    else:
        logging.debug(f"Оптимальное k найдено: {best_k_found}, с прибылью: {max_profit_for_best_k}")
    return best_k_found, best_selected_skus_details, max_profit_for_best_k


# --- Функция для жадной сборки всех возможных блоков ---
def get_all_brand_blocks(order_df):
    # Инициализация инвентаря с оставшимся количеством
    initial_inventory = order_df.copy()
    if 'original_index' not in initial_inventory.columns:
        initial_inventory = initial_inventory.reset_index().rename(columns={'index': 'original_index'})
    initial_inventory['remaining_quantity'] = initial_inventory['Quantity']
    logging.debug(f"Начальный инвентарь создан с {len(initial_inventory)} позициями.")

    # Переменные для глобального отслеживания лучшего решения
    best_total_blocks = 0
    best_total_profit = -1.0  # Инициализация значением, которое любой действительный доход превысит
    best_overall_selected_items = []
    best_overall_blocks_counts = {name: 0 for name in block_rules.keys()}

    # Рекурсивная вспомогательная функция для поиска наилучшей комбинации блоков
    def _find_best_combination(current_inventory_df, current_blocks_count_map, current_profit, current_selected_items_details, depth=0):
        nonlocal best_total_blocks, best_total_profit, best_overall_selected_items, best_overall_blocks_counts
        logging.debug(f"Глубина рекурсии: {depth}, Текущие блоки: {current_blocks_count_map}, Текущая прибыль: {current_profit:.2f}")

        # Базовый случай: нельзя сформировать больше блоков
        can_form_any_block = False
        for block_name in block_rules.keys():
            rules = block_rules[block_name]
            needed_toys = rules["toys"]
            needed_bowls = rules["bowls"]
            k_value, _, _ = find_block_set_details(current_inventory_df, needed_toys, needed_bowls)
            if k_value > 0:
                can_form_any_block = True
                break

        if not can_form_any_block:
            # Оцените текущую комбинацию: сначала максимизируем прибыль, затем количество блоков
            total_blocks_in_this_path = sum(current_blocks_count_map.values())
            logging.debug(f"Базовый случай на глубине {depth}. Оценка комбинации: блоки={total_blocks_in_this_path}, прибыль={current_profit:.2f}")
            if current_profit > best_total_profit:
                if best_total_profit > 0:
                    logging.info(f"Найдено лучшее решение по прибыли: {current_profit:.2f} (ранее {best_total_profit:.2f}). Блоков: {total_blocks_in_this_path}.")
                best_total_profit = current_profit
                best_total_blocks = total_blocks_in_this_path
                best_overall_selected_items = list(current_selected_items_details)
                best_overall_blocks_counts = current_blocks_count_map.copy()
            elif current_profit == best_total_profit:
                if total_blocks_in_this_path < best_total_blocks:
                    logging.info(f"Найдено лучшее решение по меньшему количеству блоков при той же прибыли: {total_blocks_in_this_path} (ранее {best_total_blocks}). Прибыль: {current_profit:.2f}.")
                    best_total_blocks = total_blocks_in_this_path
                    best_overall_selected_items = list(current_selected_items_details)
                    best_overall_blocks_counts = current_blocks_count_map.copy()
            return

        # Рекурсивный шаг: попробуйте сформировать каждый возможный тип блока
        for block_name in block_rules.keys():
            logging.debug(f"Глубина {depth}: Попытка сформировать блок типа {block_name}")
            rules = block_rules[block_name]
            needed_toys = rules["toys"]
            needed_bowls = rules["bowls"]

            # Используем find_block_set_details для получения оптимального k и одного набора SKU
            k_value, selected_skus_details, profit_for_set = find_block_set_details(current_inventory_df, needed_toys, needed_bowls)

            if k_value > 0:
                logging.debug(f"Глубина {depth}: Сформирован 1 набор блока типа {block_name} с k={k_value} и прибылью {profit_for_set:.2f}.")
                # Создаем новое состояние инвентаря для следующей итерации рекурсии
                next_inventory_df = current_inventory_df.copy()
                for sku_item in selected_skus_details:
                    next_inventory_df.loc[next_inventory_df['original_index'] == sku_item['original_index'], 'remaining_quantity'] -= k_value

                # Обновляем счетчики блоков и прибыль
                next_blocks_count_map = current_blocks_count_map.copy()
                next_blocks_count_map[block_name] += k_value

                next_profit = current_profit + profit_for_set
                next_selected_items_details = current_selected_items_details + [{'original_index': s['original_index'], 'k_value': k_value} for s in selected_skus_details]

                # Рекурсивный вызов
                _find_best_combination(next_inventory_df, next_blocks_count_map, next_profit, next_selected_items_details, depth + 1)

    # Запуск рекурсивной функции
    _find_best_combination(initial_inventory, {name: 0 for name in block_rules.keys()}, 0.0, [])
    if best_total_profit>0:
        logging.info(f"Завершение get_all_brand_blocks. Лучшая комбинация: Блоков={best_total_blocks}, Прибыль={best_total_profit:.2f}")
    return pd.Series({**best_overall_blocks_counts, 'selected_item_details': best_overall_selected_items})


# --- Основная функция подсчёта блоков ---
def count_brand_blocks(order_df):
    return get_all_brand_blocks(order_df)


# --- Основной pipeline---
def process_file(input_path="src/Mr. Kranch.xlsx", output_path="Mr. Kranch_calc.xlsx"):
    df = pd.read_excel(input_path)
    df.columns = df.columns.str.strip()

    # Добавляем оригинальный индекс для отслеживания перед любыми группировками/операциями
    df = df.reset_index().rename(columns={'index': 'original_index'})

    # Нормализация числовых данных
    for col in ["Amount", "Price"]:
        df[col] = df[col].astype(str).str.replace(',', '.').astype(float)

    validate_dataframe(df)

    writer = pd.ExcelWriter(output_path, engine="xlsxwriter")

    # Подсчёт блоков для каждого заказа
    order_blocks = df.groupby(['Manager', 'Contract_ID', 'Order']).apply(count_brand_blocks).reset_index()

    # Обработка выбранных деталей элементов для расчета Block_Amount и Sales_sum
    all_block_items_for_cost = []
    for _, row in order_blocks.iterrows():
        for item_data in row['selected_item_details']:
            all_block_items_for_cost.append({
                'Manager': row['Manager'],
                'Contract_ID': row['Contract_ID'],
                'Order': row['Order'],
                'original_index': item_data['original_index'],
                'k_value': item_data['k_value']
            })
    
    if not all_block_items_for_cost:
        df_calculated_items = pd.DataFrame()
    else:
        df_calculated_items = pd.DataFrame(all_block_items_for_cost)
        df_calculated_items = pd.merge(
            df_calculated_items,
            df[['Price', 'Nomenclature_ID']].reset_index().rename(columns={'index': 'original_index'}),
            on='original_index',
            how='left'
        )
        df_calculated_items['Block_Amount'] = df_calculated_items['Price'] * df_calculated_items['k_value']
    
    # Используем df_calculated_items для Sales_sum и df_with_blocks для Contracts_with_blocks
    included_indices_for_contracts = df_calculated_items['original_index'].unique() if not df_calculated_items.empty else []
    df_with_blocks = df.loc[df.index.isin(included_indices_for_contracts)]

    order_blocks["Total_blocks"] = order_blocks[list(block_rules.keys())].sum(axis=1)
    logging.debug("Общее количество блоков рассчитано.")

    # Удаляем строки, где общее количество блоков равно 0, перед сохранением на лист 'full'
    initial_rows = len(order_blocks)
    order_blocks = order_blocks[order_blocks["Total_blocks"] > 0]
    if len(order_blocks) < initial_rows:
        logging.info(f"Удалено {initial_rows - len(order_blocks)} нулевых строчек из листа 'full' (заказы без блоков).")

    summary = df_calculated_items.groupby("Manager").agg(
        Sales_sum=("Block_Amount", "sum")
    ).reset_index()

    summary_contracts = df_with_blocks.groupby("Manager")["Contract_ID"].nunique().reset_index(name="Contracts_with_blocks")
    summary = summary.merge(summary_contracts, on="Manager", how="left").fillna(0) # Заполняем NaN для менеджеров без блоков

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


# --- Запуск выполнения ---
if __name__ == "__main__":
    process_file()
