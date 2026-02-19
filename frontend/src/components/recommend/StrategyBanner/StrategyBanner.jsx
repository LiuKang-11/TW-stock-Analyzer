import "./StrategyBanner.css";

function StrategyBanner() {
  return (
    <div className="strategy-content">
      <div className="strategy-inner">
        <h2>結合法人籌碼，掌握穩健波段機會</h2>

        <p className="strategy-desc">
          依據客觀篩選條件與市場資料，提供可觀察的推薦標的。
          協助您掌握波段趨勢，做出更理性、可靠的決策。
        </p>

        <ul className="strategy-points">
          <li>即時追蹤外資與法人買賣動向</li>
          <li>標示近期趨勢結構與波段訊號</li>
          <li>可視化價格與法人成本變化</li>
        </ul>
      </div>
    </div>
  );
}

export default StrategyBanner;
