# Mr. Kranch Brand Blocks Analyzer

## Описание проекта
Данный скрипт автоматизирует обработку данных о продажах и формирует рейтинг участников программы **Mr. Kranch**.  
Основная задача – определить, сколько **бренд-блоков** собрал каждый менеджер, и составить рейтинг по количеству блоков, количеству уникальных договоров и сумме продаж.

Бренд-блоки определяются по следующим правилам:

- **Play** – 20 SKU категории *Игрушки*  
- **Happy Launch** – 40 SKU категории *Игрушки*  
- **Discovery** – 25 SKU категории *Игрушки* + 15 SKU категории *Миски*  

## Основной функционал
1. **Загрузка данных** из Excel (`Mr. Kranch.xlsx`).  
2. **Предобработка данных**:
   - Очистка названий столбцов.
   - Преобразование числовых значений (`Amount`, `Price`) в `float`.
3. **Подсчет бренд-блоков**:
   - Для каждого заказа товары сортируются по убыванию цены.
   - Проверяется, сколько блоков можно собрать (приоритет – дорогие товары).
   - Учитываются все возможные комбинации (например, если заказ содержит 60 SKU игрушек → 1 блок Happy Launch + 1 блок Play).
   - В расчет включаются только товары, вошедшие в бренд-блоки.
4. **Формирование сводки (`summary`)**:
   - Количество уникальных договоров с бренд-блоками.
   - Общая сумма продаж товаров, входящих в бренд-блоки.
   - Общее количество собранных бренд-блоков.
5. **Ранжирование менеджеров**:
   - Для каждого участника вычисляется место в рейтинге по трём показателям:
     - Количество бренд-блоков.
     - Количество договоров.
     - Сумма продаж.
   - Итоговый балл = сумма мест по трём метрикам.
   - Побеждает менеджер с **минимальным баллом**.
6. **Сохранение результатов** в Excel:
   - `Mr. Kranch_full.xlsx` – детализированные данные по заказам и блокам.
   - `Mr. Kranch_calc.xlsx` – сводная таблица по менеджерам.
   - `Mr. Kranch_ranking.xlsx` – итоговый рейтинг.

## Входные данные
Файл `Mr. Kranch.xlsx` должен содержать следующие столбцы:
- **Manager** – менеджер, оформивший заказ.  
- **Contract_ID** – номер договора.  
- **Order** – идентификатор заказа.  
- **Nomenclature_ID** – ID товара.  
- **Category** – категория товара (*Игрушки* или *Миски*).  
- **Quantity** – количество SKU в заказе.  
- **Amount** – сумма продажи.  
- **Price** – цена за единицу.  

## Выходные файлы
- `Mr. Kranch_full.xlsx` – данные по каждому заказу с учетом собранных блоков.  
- `Mr. Kranch_calc.xlsx` – агрегированная сводка по менеджерам.  
- `Mr. Kranch_ranking.xlsx` – таблица с итоговым рейтингом участников.  

## Как использовать
1. Скопировать скрипт в Jupyter Notebook или Python-файл.  
2. Убедиться, что входной файл `Mr. Kranch.xlsx` находится в рабочей директории.  
3. Запустить скрипт.
```
python Mr. Kranch_calc.py
```

4. Результаты автоматически сохраняются в Excel-файлы.  

## Пример работы
Допустим, участвуют три менеджера:

| Показатель         | Менеджер 1 | Менеджер 2 | Менеджер 3 |
|--------------------|------------|------------|------------|
| Кол-во блоков      | 2          | 3          | 1          |
| Кол-во договоров   | 2          | 3          | 1          |
| Сумма продаж       | 7,577      | 17,267     | 3,789      |

Ранжирование даст такие места:

| Менеджер   | Rank_blocks | Rank_contracts | Rank_sales | Total_score |
|------------|-------------|----------------|------------|-------------|
| Менеджер 1 | 2           | 2              | 2          | 6           |
| Менеджер 2 | 1           | 1              | 1          | 3           |
| Менеджер 3 | 3           | 3              | 3          | 9           |

🏆 Победителем становится **Менеджер 2** с минимальным итоговым баллом.

## Функции
1. Выявление и подсчёт бренд-блоков
```python
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
```
2. Создание рейтинговой таблицы на основе таблицы с бренд-блоками
```
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
```