import "./HomeIntro.css";

function HomeIntro() {
  return (
    <div className="home-intro">
      <div className="home-intro-container">
        <section className="home-intro-section">
          <h2>投資不缺資訊，缺的是判斷依據</h2>
          <p>
            市場上有大量新聞、指標與討論，但真正困難的是：
            <br />
            <strong>什麼時候該看、該不該進場、風險在哪裡。</strong>
          </p>

          <div className="feature-grid">
            <div className="feature-card">
              <h3>資訊過多</h3>
              <p>盤中消息、社群討論快速變化，新手難以判斷重點。</p>
            </div>
            <div className="feature-card">
              <h3>情緒干擾</h3>
              <p>追高、恐慌賣出，往往不是策略問題，而是決策失序。</p>
            </div>
            <div className="feature-card">
              <h3>缺乏系統</h3>
              <p>沒有固定邏輯與篩選條件，很難持續檢視績效。</p>
            </div>
          </div>
        </section>

        <section className="home-intro-section">
          <h2>用策略與篩選，讓決策更有依據</h2>
          <p>
            TW Stock Analyzer 將市場資料轉化為可理解的策略與條件，
            協助你在不同情境下，做出更理性的選擇。
          </p>

          <div className="feature-grid">
            <div className="feature-card">
              <h3>每日策略推薦</h3>
              <p>依據既定邏輯，提供當日可觀察的標的與風險區間。</p>
            </div>
            <div className="feature-card">
              <h3>自選股篩選</h3>
              <p>用條件而非感覺，建立屬於自己的觀察清單。</p>
            </div>
            <div className="feature-card">
              <h3>資訊視覺化</h3>
              <p>關鍵數據集中呈現，避免被不必要的訊息干擾。</p>
            </div>
          </div>
        </section>

        <section className="home-intro-section action">
          <h2>現在就開始體驗</h2>
          <p>
            不需要先註冊，你可以直接查看今日推薦，
            或使用自選股工具感受系統如何運作。
          </p>

          <div className="home-intro-actions">
            <button className="btn btn-primary">查看今日推薦</button>
            <button className="btn btn-secondary">前往自選股</button>
          </div>
        </section>
      </div>
    </div>
  );
}

export default HomeIntro;
