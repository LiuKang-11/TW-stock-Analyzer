import "./Hero.css";
import { useMemo } from "react";

/**
 * @typedef {Object} StockGeneratorOptions
 * @property {number} [width=800] SVG 總寬度
 * @property {number} [height=400] SVG 總高度
 * @property {number} [points=70] 採樣點數量（點越多，線條越細膩，但太高會影響效能）
 * @property {number} [startY=340] 起始 Y 座標（數值越大越靠近畫面底部）
 * @property {number} [endY=70] 結束 Y 座標（數值越小越靠上）
 * @property {number} [volatility=35] 每一步的波動幅度（數字越大，線條抖動程度激烈）
 */

/**
 * 產生模擬真實股價的上升走勢點位
 * - X 軸等距分布，Y 軸以線性上升趨勢為基礎，計算每段平均長度（xSpacing）與段的數量（points - 1）
 * - 疊加隨機波動（shock）與慣性（inertia），避免走勢過於僵硬
 * - 波動幅度會週期性調整，使視覺更接近市場節奏
 * @param {StockGeneratorOptions} options - 設定參數物件
 * @returns {Array<{ x: number, y: number }>} 股價路徑點陣列
 */
function generateStockPoints({
  width = 800,
  height = 400,
  points = 70,
  startY = 340,
  endY = 70,
  volatility = 35,
}) {
  const stockPointsArray = [];
  const xSpacing = width / (points - 1);

  // 每個點的線性上升量，線性上升趨勢：從 startY 漸漸走到 endY
  const trendPerStep = (endY - startY) / (points - 1);

  let currentY = startY;
  let currentVolatility = volatility; // 波動變化，讓波動有一段大、一段小（更像市場）

  for (let i = 0; i < points; i++) {
    const xPosition = i * xSpacing;

    // 偶爾讓波動變大/變小
    if (i % 12 === 0 && i !== 0) {
      currentVolatility = volatility * (0.6 + Math.random() * 1.2);
    }

    const shock = (Math.random() - 0.5) * currentVolatility;
    const inertia = (Math.random() - 0.5) * (currentVolatility * 0.35);

    currentY = currentY + trendPerStep + shock + inertia;

    // 讓 currentY 不要超出 viewBox
    currentY = Math.max(20, Math.min(height - 20, currentY));

    stockPointsArray.push({ x: xPosition, y: currentY });
  }

  // 確保最後收在接近 endY（更穩定）
  stockPointsArray[stockPointsArray.length - 1].y = endY;

  return stockPointsArray;
}

/**
 * 將股價折線點轉換為平滑的 SVG Bezier path（Catmull-Rom 曲線）
 * p0 ~ p3 = 計算貝茲曲線控制點的前後參考點
 * @param {Array<{ x: number, y: number }>} pointsArray 
 * @returns {string} 直接丟給 SVG <path d="..." /> 使用的字串
 */
function catmullRomToBezierPath(pointsArray) {
  if (!pointsArray || pointsArray.length < 2) return "";

  let pathD = `M ${pointsArray[0].x} ${pointsArray[0].y}`;

  // 透過參考當前點 (p1) 的前一點 (p0) 與後一點 (p2) 來計算出切線方向
  // 將此切線長度取 1/6 作為貝茲曲線的控制點偏移量，能產生最自然的平滑感
  // 計算下一點 (p2) 的入彎控制點，參考點為 p1 與 p3
  // 最後將 p1 到 p2 的直線段，替換為帶有兩個控制點的三次貝茲曲線
  for (let i = 0; i < pointsArray.length - 1; i++) {
    const p0 = pointsArray[i - 1] || pointsArray[i];
    const p1 = pointsArray[i];
    const p2 = pointsArray[i + 1];
    const p3 = pointsArray[i + 2] || p2;

    const controlPoint1X = p1.x + (p2.x - p0.x) / 6;
    const controlPoint1Y = p1.y + (p2.y - p0.y) / 6;
    const controlPoint2X = p2.x - (p3.x - p1.x) / 6;
    const controlPoint2Y = p2.y - (p3.y - p1.y) / 6;

    pathD += ` C ${controlPoint1X} ${controlPoint1Y}, ${controlPoint2X} ${controlPoint2Y}, ${p2.x} ${p2.y}`;
  }

  return pathD;
}

function Hero() {
  /**
   * 計算趨勢線與淡淡的下方填色區域的 SVG path（僅初始化時計算一次）
   */
  const { linePathD, areaPathD } = useMemo(() => {
    const stockPointsArray = generateStockPoints({ width: 800, height: 400, points: 70 });
    const linePathD = catmullRomToBezierPath(stockPointsArray);
    const lastPoint = stockPointsArray[stockPointsArray.length - 1];
    const firstPoint = stockPointsArray[0];
    const chartBottomY = 400;
    const areaPathD = `${linePathD} L ${lastPoint.x} ${chartBottomY} L ${firstPoint.x} ${chartBottomY} Z`;

    return { linePathD, areaPathD };
  }, []);

  return (
    <section className="hero-section">
      <div className="hero-container">
        <div className="hero-left">
          <h1>專業股市分析平台，新手也能穩定選股</h1>
          <p>每日策略推薦 × 自選股篩選，協助你避開情緒操作</p>
          <div className="hero-actions">
            <button className="btn btn-primary" type="button">查看今日推薦</button>
            <button className="btn btn-secondary btn-on-light" type="button">免費體驗</button>
          </div>
        </div>

        <div className="hero-right" aria-hidden="true">
          <svg
            className="trend-svg"
            viewBox="0 0 800 400"
            preserveAspectRatio="xMidYMid meet"
          >
            <defs>
              <linearGradient id="trendFill" x1="0" y1="0" x2="0" y2="1">
                <stop offset="0%" stopColor="rgba(99,179,237,0.25)" />
                <stop offset="100%" stopColor="rgba(99,179,237,0)" />
              </linearGradient>
            </defs>

            <path d={areaPathD} fill="url(#trendFill)" />

            <path className="trend-line-path" d={linePathD} pathLength={1} />
          </svg>
        </div>
      </div>
    </section>
  );
}

export default Hero;
