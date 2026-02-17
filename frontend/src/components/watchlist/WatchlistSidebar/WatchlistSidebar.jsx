import "./WatchlistSidebar.css";

function WatchlistSidebar({ watchlistGroups = [] }) {
  return (
    <div className="watchlist-sidebar">
      <div className="sidebar-title">自選群組</div>

      <div className="group-list">
        {watchlistGroups.length > 0 ? (
          watchlistGroups.map((group) => (
            <div key={group.id} className="group-item">
              <div className="group-main">
                <span className="drag-handle">⋮⋮</span>
                <span className="group-name">{group.name}</span>
                <span className="count">{group.stockCount}</span>
              </div>
              <div className="group-actions">
                <button className="icon-btn edit">✎</button>
                <button className="icon-btn delete">×</button>
              </div>
            </div>
          ))
        ) : (
          <div className="empty-state-sidebar">
            目前無群組
            <span>請點擊下方新增群組</span>
          </div>
        )}
      </div>

      <button className="add-group-btn">
        <span>+</span> 新增群組
      </button>
    </div>
  );
}

export default WatchlistSidebar;
