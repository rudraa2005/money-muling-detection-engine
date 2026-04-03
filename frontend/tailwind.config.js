/** @type {import('tailwindcss').Config} */
export default {
  content: [
    "./index.html",
    "./src/**/*.{js,ts,jsx,tsx}",
  ],
  darkMode: "class",
  theme: {
    extend: {
      colors: {
        "primary": "#f3efe6",
        "accent-blue": "#748296",
        "accent-purple": "#8b7f74",
        "accent-red": "#ad735d",
        "surface": "#111214",
        "surface-highlight": "#17191c",
        "border-subtle": "#2b3036",
        "text-muted": "#9a9b93",
        "background-dark": "#0c0d10",
        "card-dark": "#15171a",
      },
      fontFamily: {
        "display": ["Instrument Serif", "serif"],
        "body": ["Instrument Sans", "sans-serif"],
        "technical": ["JetBrains Mono", "monospace"],
      },
      animation: {
        "spin-slow": "spin 15s linear infinite",
      },
    },
  },
  plugins: [],
}
