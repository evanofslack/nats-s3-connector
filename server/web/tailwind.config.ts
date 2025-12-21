import type { Config } from "tailwindcss";

export default {
  content: ["./index.html", "./src/**/*.{js,ts,jsx,tsx}"],
  theme: {
    extend: {
      colors: {
        bg: {
          main: "var(--color-bg-main)",
          panel: "var(--color-bg-panel)",
        },
        border: {
          subtle: "var(--color-border-subtle)",
        },
        text: {
          primary: "var(--color-text-primary)",
          muted: "var(--color-text-muted)",
        },
        accent: {
          DEFAULT: "var(--color-accent)",
          alt: "var(--color-accent-alt)",
        },
        status: {
          created: "var(--color-status-created)",
          running: "var(--color-status-running)",
          paused: "var(--color-status-paused)",
          success: "var(--color-status-success)",
          failure: "var(--color-status-failure)",
        },
        success: "var(--color-success)",
        warning: "var(--color-warning)",
        error: "var(--color-error)",
      },
    },
  },
  plugins: [],
} satisfies Config;
