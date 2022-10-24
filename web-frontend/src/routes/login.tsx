import Box from '@mui/material/Box';
import Auth from '../components/auth';
import { useParams } from "react-router-dom";

/*
public
single user api key
login providers
    Oauth2
    Basic
*/




const Login = () => {
      // Extract from path from react-router.
    const params = useParams<{ "*": string }>();
    // Transform "/a/b/c" to ["a", "b", "c"].
    const segments = (params["*"] || "").split("/").filter(function (segment) {
        return segment;
    });
    console.log(`segments: ${segments}`)
    return (
        // list of buttons of providers to choose from
        <Auth/>

    );
}

export default Login
