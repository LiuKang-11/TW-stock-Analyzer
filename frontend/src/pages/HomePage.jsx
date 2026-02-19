import Hero from "../components/home/Hero/Hero";
import HomeIntro from "../components/home/HomeIntro/HomeIntro";

function HomePage() {
  return (
    <main className="page">
      <section className="hero-section">
        <div className="container">
          <Hero />
        </div>
      </section>

      <section className="home-intro">
        <div className="container">
          <HomeIntro />
        </div>
      </section>
    </main>
  );
}

export default HomePage;
