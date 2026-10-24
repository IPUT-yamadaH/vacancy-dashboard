# 空席状況ダッシュボード

FlaskでTCPソケットから感圧センサーの値を受け取り、座席状況、曜日ごとの空席予測、時間別実績、管理者ログを表示します。実機センサーは1番席、2〜20番席と予測の一部はデモ用データです。

## 必要環境

- Python 3.10以上
- PostgreSQL (Supabase等)
- センサー送信機からアプリへのTCP接続

## 起動

GitHubにコードを上げるだけなら、実際の `.env` は不要です。DBを使って起動するときに、設定例をコピーして接続情報を入れてください。


```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
pip install -r requirements.txt
Copy-Item .env.example .env
# .envを編集
python app.py
```

画面は `http://127.0.0.1:5001`、センサー受信はTCPポート `8765` です。管理者機能を使う場合は `.env` に `ADMIN_PASSWORD` と `FLASK_SECRET_KEY` を設定してください。パスワードが未設定の場合は管理者ログインを拒否します。

## データベース

アプリ起動時に `daily_sensoe_log` テーブルは作成されます（既存DBとの互換性のため、このテーブル名を使用しています）。予測・実績画面には次のテーブルも必要です:

```sql
CREATE TABLE IF NOT EXISTS daily_vacancy_status (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    time timestamptz NOT NULL,
    target_week text,
    day_of_week text,
    predicted_vacancy integer
);
```

## センサー送信形式

1接続につきJSONを1件送ります。`force`は感圧値、`id`は任意の端末識別子です。

```json
{"id":"seat-1","force":620}
```

しきい値は `OCCUPIED_THRESHOLD=500`、しきい値を下回ってから20分は「退席中かも」と表示します。運用環境に合わせてコードの定数を変更できます。

## 公開時の注意

`.env`やDB資格情報はGitへ追加しないでください。HTTPSで公開する場合は `USE_HTTPS=true` に設定してください。センサー受信ポートをインターネットへ直接公開せず、信頼できるネットワークからだけ到達可能にしてください。
