#!/bin/bash

echo "======================= $0 START ======================="

cd /home/ubuntu/TradeApp
echo "Current Working Directory : "$(pwd)

## funct_list=("ic_watchlist.py" "pcr.py" "funds.py" "trade_app.py" "ic_ema_strategy.py" "update_scrips.py" "strategy_check.py")
funct_list=("ic_watchlist.py" "pcr.py" "funds.py" "trade_app.py" "strategies.py" "update_scrips.py" "strategy_check.py")

for item in "${funct_list[@]}"
do
        echo "Function Name - $item"
        if ! pgrep -f "python" > /dev/null; then
                nohup python3 $item &
                echo "Process ID - "$(pgrep --full "python3 $item")
        else
                if [[ -z $(pgrep --full "python3 $item") ]]; then
                        nohup python3 $item &
                        echo "New Process ID - "$(pgrep --full "python3 $item")
                else
                        echo "Previous Process ID - "$(pgrep --full "python3 $item")
                        kill $(pgrep --full "python3 $item")
                        nohup python3 $item &
                        echo "New Process ID - "$(pgrep --full "python3 $item")
                fi
        fi
done
echo "======================= $0 END ======================="
exit 0