/**
 * SPDX-FileCopyrightText: Copyright (c) NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 * SPDX-License-Identifier: Apache-2.0
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import type { QueryClient } from "@tanstack/react-query";
import {
	createRootRouteWithContext,
	createRoute,
} from "@tanstack/react-router";
import { screen } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { TROUBLESHOOTING_DOCS_URL } from "~/components/UnreachableServiceFallback";
import { HttpError } from "~/lib/fetch";
import { renderWithRouter } from "~/testing/render";

function httpError(status: number, statusText: string) {
	return new HttpError({ status, statusText } as Response, null);
}

// The fallback reads router context, so the error is raised from a real route
// loader and caught by the router's defaultErrorComponent.
function renderWithLoaderError(error: unknown) {
	const rootRoute = createRootRouteWithContext<{ queryClient: QueryClient }>()(
		{},
	);
	const indexRoute = createRoute({
		getParentRoute: () => rootRoute,
		path: "/",
		loader: () => {
			throw error;
		},
		component: () => null,
	});

	return renderWithRouter({
		routeTree: rootRoute.addChildren([indexRoute]),
		initialLocation: "/",
	});
}

beforeEach(() => {
	// TanStack Router logs every caught loader error.
	vi.spyOn(console, "error").mockImplementation(() => {});
});

afterEach(() => {
	vi.restoreAllMocks();
});

describe("RouteErrorFallback", () => {
	it("explains an unreachable service instead of showing the 502 status", async () => {
		renderWithLoaderError(httpError(502, "Bad Gateway"));

		expect(
			await screen.findByText("Unable to reach the deployed service"),
		).toBeInTheDocument();
		expect(
			screen.getByText(/Refer to the troubleshooting documentation/),
		).toBeInTheDocument();
		expect(screen.queryByText("502 Bad Gateway")).not.toBeInTheDocument();
	});

	it("links the unreachable state to the troubleshooting docs", async () => {
		renderWithLoaderError(httpError(502, "Bad Gateway"));

		const link = await screen.findByRole("link", {
			name: /View Documentation/,
		});
		expect(link).toHaveAttribute("href", TROUBLESHOOTING_DOCS_URL);
	});

	it("keeps the generic fallback for other HTTP errors", async () => {
		renderWithLoaderError(httpError(500, "Internal Server Error"));

		expect(
			await screen.findByRole("button", { name: "Try again" }),
		).toBeInTheDocument();
		expect(
			screen.queryByText("Unable to reach the deployed service"),
		).not.toBeInTheDocument();
	});

	it("keeps the generic fallback for non-HTTP errors", async () => {
		renderWithLoaderError(new Error("No account found"));

		expect(await screen.findByText("No account found")).toBeInTheDocument();
		expect(
			screen.getByRole("button", { name: "Try again" }),
		).toBeInTheDocument();
	});
});
