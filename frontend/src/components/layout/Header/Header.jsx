import "./Header.css";

function Header() {
  return (
    <header className="header-background">
      <div className="header-container">
        <div className="brand-logo">TW-stock-Analyzer</div>
        <div className="menu">
          <div className="menu-group">
            <div className="menu-item active">首頁</div>
            <div className="menu-item">推薦股</div>
            <div className="menu-item">自選股</div>
          </div>
        </div>
      </div>
    </header>
  );
}

export default Header;
