

# You can pass directories to ruff directly, and it will recursively check all files in them.
# If you want to include both specific files and entire directories, simply include them in the list.


targets=(
    web_scripts/trade_client
    web_scripts/nse_vol_tracker
    tradeapi
    utils
    tradeview
    orchest
)


ruff check --fix "${targets[@]}"
ruff format "${targets[@]}"

# mypy web_scripts/trade_client

frontend_targets=(
    web_scripts/templates/template_trade_client/dashboard.html
)

djlint --lint "${frontend_targets[@]}"
# djlint --reformat "${frontend_targets[@]}"
