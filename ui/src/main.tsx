/**
 * Entry point: mounts the app and initializes global state. Styles are
 * imported here in token -> base -> app order so the cascade is predictable.
 */

import { render } from "preact";
import { App } from "./app";
import { initStore } from "./state/store";
import "./styles/tokens.css";
import "./styles/base.css";
import "./styles/app.css";

const mount = document.getElementById("app");
if (mount === null) {
	throw new Error("dsui: mount target #app not found in document");
}

initStore();
render(<App />, mount);
