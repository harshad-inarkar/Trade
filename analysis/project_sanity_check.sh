

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


echo "Running ruff------"
ruff check --fix "${targets[@]}"
ruff format "${targets[@]}"


echo "Running mypy------"
mypy "${targets[@]}"




echo "Running djlint------"
frontend_targets=(
    web_scripts/templates
)

djlint --lint "${frontend_targets[@]}"
# djlint --reformat "${frontend_targets[@]}"
