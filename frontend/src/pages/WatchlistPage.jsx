import React, { useState } from "react";
import WatchlistSearch from "../components/watchlist/WatchlistSearch/WatchlistSearch";
import WatchlistSidebar from "../components/watchlist/WatchlistSidebar/WatchlistSidebar";
import WatchlistTable from "../components/watchlist/WatchlistTable/WatchlistTable";
import "../styles/layout/WatchlistPage.css";

function WatchlistPage() {
  const [watchlistGroups] = useState([]);
  const [watchlistStocks] = useState([]);

  return (
    <main className="page">
      <section className="watchlist-section">
        <div className="container">
          <div className="watchlist-page">
            <div className="watchlist-page-top-bar">
              <WatchlistSearch />
            </div>

            <div className="watchlist-page-content">
              <aside className="watchlist-page-sidebar">
                <WatchlistSidebar watchlistGroups={watchlistGroups} />
              </aside>

              <section className="watchlist-page-main">
                <WatchlistTable watchlistStocks={watchlistStocks} />
              </section>
            </div>
          </div>
        </div>
      </section>
    </main>
  );
}

export default WatchlistPage;
