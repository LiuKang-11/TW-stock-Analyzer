import "./WatchlistSearch.css";

function WatchlistSearch() {
  return (
    <div className="watchlist-search">
      <input
        className="search-input"
        type="text"
        placeholder="搜尋股票代號 / 名稱"
      />
    </div>
  );
}

export default WatchlistSearch;
