from lineage.ids import split_column_id


def test_four_part_name():
    assert split_column_id("main.raw.orders.amount") == ("main.raw.orders", "amount")


def test_three_part_name():
    assert split_column_id("raw.orders.amount") == ("raw.orders", "amount")


def test_two_part_name():
    assert split_column_id("orders.amount") == ("orders", "amount")


def test_no_dot_returns_empty_column():
    assert split_column_id("amount") == ("amount", "")


def test_regression_rsplit_not_split():
    # split(".", 1) on "main.raw.orders.amount" gives ("main", "raw.orders.amount") — wrong
    table, col = split_column_id("main.raw.orders.amount")
    assert table == "main.raw.orders", f"expected 'main.raw.orders', got {table!r}"
    assert col == "amount"
