import "./TodayRecommend.css";
import StockRow from "../StockRow/StockRow";

function TodayRecommend({ stocks = [] }) {
  return (
    <div className="today-recommend-container">
      <header className="recommend-header">
        <h2>今日推薦標的</h2>
        <p>依據法人籌碼與成本結構，篩選具備波段潛力的觀察名單</p>
      </header>

      <section className="recommend-group">
        <div className="stock-list">
          {stocks.length === 0 ? (
            <div className="empty-state">目前尚無推薦標的</div>
          ) : (
            stocks.map((stock) => <StockRow key={stock.id} stock={stock} />)
          )}
        </div>
      </section>
    </div>
  );
}

export default TodayRecommend;
