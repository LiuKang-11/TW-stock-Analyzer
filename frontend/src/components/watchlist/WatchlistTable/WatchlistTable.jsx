import "./WatchlistTable.css";

function WatchlistTable({ watchlistStocks = [] }) {
  return (
    <div className="watchlist-table">
      <div className="table-header">
        <div className="col-stock-header">股票 / 代號</div>
        <div className="col-price">現價</div>
        <div className="col-action"></div>
      </div>

      <div className="table-body">
        {watchlistStocks.length > 0 ? (
          watchlistStocks.map((stock) => (
            <div key={stock.id} className="row">
              <div className="col-drag">
                <span className="drag-handle">⋮⋮</span>
              </div>
              <div className="col-stock">
                <span className="name">{stock.stockName}</span>
                <span className="code">{stock.stockCode}</span>
              </div>
              <div className="col-price">
                {stock.currentPrice?.toLocaleString()}
              </div>
              <div className="col-action">
                <button className="delete-btn">×</button>
              </div>
            </div>
          ))
        ) : (
          <div className="empty-state-main">
            尚未建立觀察清單
            <span>透過搜尋加入股票，建立你的觀察組合</span>
          </div>
        )}
      </div>
    </div>
  );
}

export default WatchlistTable;
