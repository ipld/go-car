import { defineConfig, godoc, markdown } from "sourcey";

export default defineConfig({
  name: "go-car Documentation",
  repo: "https://github.com/ipld/go-car",
  prettyUrls: "slash",
  theme: {
    preset: "default",
    colors: {
      primary: "#1769aa",
      light: "#4f9bd3",
      dark: "#0b3d66",
    },
  },
  navbar: {
    links: [
      { label: "Source", href: "https://github.com/ipld/go-car" },
      { label: "Releases", href: "https://github.com/ipld/go-car/releases" },
    ],
  },
  navigation: {
    tabs: [
      {
        tab: "Guides",
        slug: "",
        source: markdown({
          groups: [
            {
              group: "Getting Started",
              pages: ["introduction", "choosing-a-version", "common-workflows"],
            },
          ],
        }),
      },
      {
        tab: "v2 API",
        slug: "v2",
        source: godoc({
          module: "../../v2",
          packages: ["./..."],
          includeTests: true,
          hideUndocumented: false,
        }),
      },
      {
        tab: "v1 API",
        slug: "v1",
        source: godoc({
          module: "../..",
          packages: ["./..."],
          includeTests: true,
          hideUndocumented: false,
          exclude: ["./v2/..."],
        }),
      },
    ],
  },
});
