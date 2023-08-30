import * as React from "react";
import { Paper, withStyles, Grid, TextField, Button, FormControlLabel, Checkbox } from '@mui/material';
import {axiosInstance} from '../client';
import {ACCESS_TOKEN_KEY, REFRESH_TOKEN_KEY} from "../client";
import { useContext, useState } from "react";
import { SettingsContext } from "../context/settings";

interface Props{
}

function Auth (props: Props) {
    const settings = useContext(SettingsContext);
    async function login(){
        const loginData = new FormData();
        loginData.append("username", username);
        loginData.append("password", password);

        const response = await axiosInstance.post(`${settings.api_url}/auth/provider/toy/token`, loginData, {

            headers: {
                "Content-Type": "multipart/form-data"
            }});

        localStorage.setItem(REFRESH_TOKEN_KEY, response.data.refresh_token);
        localStorage.setItem(ACCESS_TOKEN_KEY, response.data.access_token);

        //update user context
        console.log(response.data.identity.id);
    }

    const [username, setUsername] = useState<string>("");
    const [password, setPassword] = useState<string>("");

    return (

            <Paper
                sx={{  mx: "auto",
                width: 400,
                display: "flex",
                flexDirection: "column",
                padding: "20px",
                marginTop: "100px",
                typography: "body1",
                border: 1,
                borderRadius: "16px"
            }}

            >
                <div >
                    <Grid container spacing={8} alignItems="flex-end">

                        <Grid item md={true} sm={true} xs={true}>
                            <TextField
                            id="username"
                            label="Username"
                            type="email" fullWidth autoFocus required
                            value={username}
                            onChange={(event: React.ChangeEvent<HTMLInputElement>) => setUsername(event.target.value)}/>
                        </Grid>
                    </Grid>
                    <Grid container spacing={8} alignItems="flex-end">

                        <Grid item md={true} sm={true} xs={true}>
                            <TextField
                                id="username"
                                label="Password"
                                type="password"  value={password} fullWidth required
                                onChange={(event: React.ChangeEvent<HTMLInputElement>) => setPassword(event.target.value)}
                                onKeyPress={(event) => {
                                    if (event.key === "Enter") {
                                        login();
                                    }
                                  }}/>
                        </Grid>
                    </Grid>

                    <Grid container  sx={{ marginTop: '10px' }}>
                        <Button
                            variant="outlined"
                            color="primary"
                            style={{ textTransform: "none" }}
                            onClick={login}>Login</Button>
                    </Grid>
                </div>
            </Paper>
        );
}

export default Auth;
