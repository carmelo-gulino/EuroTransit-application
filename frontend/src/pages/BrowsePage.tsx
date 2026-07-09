import { useEffect, useState } from "react";
import { useNavigate } from "react-router-dom";
import { listRoutes, listOffers, type Route, type Offer } from "../api/catalog";
import { ApiError } from "../api/client";

/** Public browse page: routes + offers, no auth required (catalog reads are public). */
export function BrowsePage() {
  const [routes, setRoutes] = useState<Route[]>([]);
  const [offers, setOffers] = useState<Offer[]>([]);
  const [error, setError] = useState<string | null>(null);
  const navigate = useNavigate();

  useEffect(() => {
    const ac = new AbortController();
    Promise.all([listRoutes(ac.signal), listOffers(ac.signal)])
      .then(([r, o]) => {
        setRoutes(r);
        setOffers(o);
      })
      .catch((e) => {
        if (e instanceof ApiError && e.kind === "network") return; // aborted/offline
        setError(e instanceof Error ? e.message : "Failed to load catalog");
      });
    return () => ac.abort();
  }, []);

  const offersByRoute = (routeId: string) => offers.filter((o) => o.routeId === routeId);

  return (
    <>
      <h1>Routes</h1>
      {error && <p className="error">{error}</p>}
      {routes.map((r) => (
        <article className="route-card" key={r.routeId}>
          <div className="route-card__head">
            <span className="route-card__cities">
              {r.origin}
              <span className="arrow">→</span>
              {r.destination}
            </span>
            <span className="badge">{r.trainType}</span>
          </div>

          <div className="journey">
            <span className="journey__time">{r.departureTime.slice(0, 5)}</span>
            <span className="journey__track">
              <span className="journey__dot" />
              <span className="journey__line" />
              <span className="journey__meta">{formatDuration(r.travelTime)}</span>
              <span className="journey__line" />
              <span className="journey__dot journey__dot--hollow" />
            </span>
            <span className="journey__time">{r.arrivalTime.slice(0, 5)}</span>
          </div>

          <div className="fares">
            {offersByRoute(r.routeId).map((o) => (
              <div className="fare-row" key={o.offerId}>
                <span className="fare-row__class">{o.fareClass}</span>
                <span className="fare-row__cond">{o.conditions}</span>
                <span className="price">
                  {o.price.toFixed(2)}
                  <span className="cur">{o.currency}</span>
                </span>
                <button
                  className="btn btn--sm"
                  onClick={() =>
                    navigate("/checkout", {
                      state: {
                        routeId: r.routeId,
                        price: o.price,
                        currency: o.currency,
                        origin: r.origin,
                        destination: r.destination,
                        fareClass: o.fareClass,
                      },
                    })
                  }
                >
                  Book
                </button>
              </div>
            ))}
          </div>
        </article>
      ))}
    </>
  );
}

/** ISO-8601 duration (e.g. "PT1H5M") → "1h 05". Presentation only. */
function formatDuration(iso: string): string {
  const m = /PT(?:(\d+)H)?(?:(\d+)M)?/.exec(iso);
  if (!m) return "";
  const h = m[1] ? Number(m[1]) : 0;
  const min = m[2] ? Number(m[2]) : 0;
  if (h && min) return `${h}h ${String(min).padStart(2, "0")}`;
  if (h) return `${h}h`;
  return `${min}min`;
}
