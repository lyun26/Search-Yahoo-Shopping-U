# search_yahoo_shopping
https://github.com/amaga38/search_yahoo_shopping の更新

[Yahoo! JAPAN の商品検索 API(V3)](https://developer.yahoo.co.jp/webapi/shopping/shopping/v3/itemsearch.html)を利用して Yahoo! ショッピングの店舗の商品を検索、保存するツールです。

キーワード検索 → ヒットした商品を出品している店舗の店舗ページで出品されている商品を**安い順**に昇順で、商品名、値段、店舗 URL を記録します。また、検索にヒットした店舗一覧を別ファイルに記録します。

- out/キーワード/<時間\_日付>/shop.xlsx: 店舗一覧のファイル。店舗名、店舗 URL
- out/キーワード/<時間*日付>/shop/<ショップ名*ショップ ID>.xlsx: 店舗の出品商品一覧のファイル。商品名、値段、店舗 URL

## 注意事項、制限事項

- [商品検索 API には 1 分間で 30 リクエストの制限があります](商品検索APIには1分間で30リクエストの制限があります)
- API の仕様のため、商品の検索結果で取得できる検索結果は、**1000 件が上限**です。同一の値段で 1000 件以上の商品が出品されていると、すべての商品を記録することができません

## インストール

動作確認環境 Windows 11 Home 21H2、Python 3.9.0

事前に [Python](https://www.python.org/) 3.9.0 以上のバージョンをインストール

プログラムのダウンロード: [GitHub の Zip リンク](https://github.com/amaga38/search_yahoo_shopping/archive/refs/heads/main.zip)

- 解凍したフォルダを開き、フォルダーのアドレス欄に`cmd`と入力して、Enter。コマンドターミナルを開く
- コマンドターミナルで以下のコマンドを実行して、必要な外部ライブラリをインストール

```
pip install -r requirements.txt
```

## 実行前の準備

`search_yahoo_shopping.py`と同じフォルダに以下のファイルを用意

- appid.xlsx
- keyword.xlsx

#### appid.xlsx

- Yahoo! Japan Web API を利用するために必要なアプリケーション ID を記載するファイル。プログラムを動かすためには 2 個の ID が必要
- [Yahoo! JAPAN の開発者ページ](https://e.developer.yahoo.co.jp/dashboard/)でアプリを 2 個登録する
- 「新しいアプリケーションを登録」→ 「ID 連携を利用しない」でアプリケーションを登録。ID 連携以外は利用者の情報を入力
- コピペで ID 最後のハイフン`-`が抜けやすいので注意

#### keyword.xlsx

- 検索するキーワードを A 列にそれぞれ記載したファイル
- 1 行 1 キーワードになります
- B1 セルに商品情報ファイルに出力する商品情報の最大行数を設定（デフォルト 50 万）
- C1 セルに検索して収集する店舗情報の 1 キーワードあたりの最大数を設定（デフォルト 100 店舗）

**例**

|     | A     | B      | C   | D   |
| --- | ----- | ------ | --- | --- |
| 1   | nike  | 500000 | 100 |     |
| 2   | shoes |        |     |     |
| 3   | bag   |        |     |     |

## 実行

```
python search_yahoo_shopping.py
```

keyword.xlsx に記載されたキーワードを上から順番に検索し、店舗の出品商品情報を記録していきます。

### 出力情報

出力先は、`search_yahoo_shopping.py`と同じフォルダに`out`フォルダが作成され、以下のような階層でファイルが出力されます。

```
 - search_yahoo_shopping.py
 - out
   - <実行日時>
     - shops_all.xlsx :各キーワード検索でヒットした店舗情報一覧を1つにまとめたもの (重複排除)
     - items_all_<番号>.xlsx :各店舗毎の商品情報一覧 (<店舗名>_<店舗ID>.xlsx) を1つにまとめたもの
     - <検索キーワード> : keyword.xlsxに記載されているキーワードに対応
       - shops.xlsx: 検索キーワードでヒットした店舗情報一覧。出力情報 (店舗名, 店舗URL, 店舗ID)
       - shop: 店舗毎の商品情報出力フォルダ
         - <店舗名>_<店舗ID>.xlsx : 出力情報 (商品名, 値段, 店舗URL)
```

### 店舗検索だけの実行

```
python search_yahoo_shopping_stores.py
```

### 商品検索だけの実行

検索する店舗情報が必要なので、同一フォルダに shops_all.xlsx が必要です。

```
python search_yahoo_shopping_items.py
```

または、オプションで shops_all.xlsx を指定してください。

```
python search_yahoo_shopping_items.py --shops-all-path .\out\<実行日時>\shops_all.xlsx
```
