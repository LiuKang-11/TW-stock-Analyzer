import { BrowserRouter, Routes, Route } from "react-router-dom";
import Header from "./components/layout/Header/Header";
import Footer from "./components/layout/Footer/Footer";
import HomePage from "./pages/HomePage";
import RecommendPage from "./pages/RecommendPage";
import "./styles/theme.css";
import "./styles/button.css";

function App() {
  return (
    <BrowserRouter>
      <Header />

      <Routes>
        <Route path="/" element={<HomePage />} />
        <Route path="/recommend" element={<RecommendPage />} />
      </Routes>

      <Footer />
    </BrowserRouter>
  );
}

export default App;
