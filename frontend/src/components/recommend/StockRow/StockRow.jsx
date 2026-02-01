import "./StockRow.css";

function StockRow({ stock }) {
  return (
    <div className="stock-row">
      <div className="row-left">
        <h4 className="stock-name">{stock?.name}</h4>
        <span className="stock-code">{stock?.code}</span>
      </div>

      <div className="row-right">
        <button className="detail-btn">
          查看詳細資料
          <svg
            width="16"
            height="16"
            viewBox="0 0 24 24"
            fill="none"
            stroke="currentColor"
            strokeWidth="2.5">
            <path d="M7 7l5 5-5 5M13 7l5 5-5 5" />
          </svg>
        </button>
      </div>

      <div className="row-divider"></div>
    </div>
  );
}

export default StockRow;
