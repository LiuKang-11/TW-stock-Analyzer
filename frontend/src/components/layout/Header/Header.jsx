import { NavLink } from "react-router-dom";
import "./Header.css";

function Header() {
  return (
    <header className="header-background">
      <div className="header-container">
        <NavLink to="/" end className="brand-logo">
          TW-stock-Analyzer
        </NavLink>
        <div className="menu">
          <div className="menu-group">
            <NavLink to="/" end className="menu-item">
              首頁
            </NavLink>

            <NavLink to="/recommend" className="menu-item">
              推薦股
            </NavLink>

            <NavLink to="/watchlist" className="menu-item">
              自選股
            </NavLink>
          </div>
        </div>
      </div>
    </header>
  );
}

export default Header;
