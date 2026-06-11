

# You can pass directories to ruff directly, and it will recursively check all files in them.
# If you want to include both specific files and entire directories, simply include them in the list.

targets=(
    web_scripts/trade_client/trade_app.py
    tradeapi/dhan_trade.py
    tradeapi/scrip_master.py
    tradeapi/scrip_search.py
)

ruff check --fix "${targets[@]}"
ruff format "${targets[@]}"


frontend_targets=(
    web_scripts/templates/template_trade_client/dashboard.html
)

djlint --lint "${frontend_targets[@]}"
# djlint --reformat "${frontend_targets[@]}"
