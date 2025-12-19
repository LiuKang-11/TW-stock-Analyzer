import Header from "./components/layout/Header/Header";
import Footer from "./components/layout/Footer/Footer";
import HomePage from "./pages/HomePage";
import "./styles/theme.css";
import "./styles/button.css";

function App() {
  return (
    <>
      <Header />
      <HomePage />
      <Footer />
    </>
  );
}

export default App;
